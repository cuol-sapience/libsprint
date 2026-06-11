#include "sprint/ipc/backend/posix.hpp"
#include "sprint/ipc/backend/common.hpp"

#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

static size_t page_align_up(size_t n) {
  static const size_t page = (size_t)sysconf(_SC_PAGESIZE);
  return (n + page - 1) & ~(page - 1);
}

namespace sprint::ipc {

void *PosixShmBackend::alloc(size_t bytes) {
  // generate a unique shm name using the pointer address (temp)
  // real name is set in export_handle after alloc
  void *ptr = mmap(nullptr, bytes, PROT_READ | PROT_WRITE,
                   MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED)
    throw std::runtime_error("mmap anonymous: " + std::string(strerror(errno)));
  return ptr;
};

void PosixShmBackend::free_ptr(void *ptr, size_t bytes) {
  if (ptr)
    munmap(ptr, bytes);
};

// For the POSIX backend, alloc_named() replaces alloc() + export_handle()
// because we need a name for the shm object up front.
// slot_index==0 creates a single shm file sized for all ring slots;
// subsequent calls open the existing file and map at the slot's page-aligned offset.
std::pair<void *, RawHandle>
PosixShmBackend::alloc_named(const std::string &base, uint32_t slot_index,
                             uint32_t ring_size, size_t bytes) {
  char shm_name[64];
  snprintf(shm_name, sizeof(shm_name), "%.56s_data", base.c_str());

  size_t aligned = page_align_up(bytes);
  size_t slot_offset = (size_t)slot_index * aligned;

  int fd;
  if (slot_index == 0) {
    shm_unlink(shm_name);
    fd = shm_open(shm_name, O_CREAT | O_RDWR, 0666);
    if (fd < 0)
      throw std::runtime_error("shm_open: " + std::string(strerror(errno)));
    if (ftruncate(fd, (off_t)(aligned * ring_size)) < 0)
      throw std::runtime_error("ftruncate: " + std::string(strerror(errno)));
  } else {
    fd = shm_open(shm_name, O_RDWR, 0666);
    if (fd < 0)
      throw std::runtime_error("shm_open: " + std::string(strerror(errno)));
  }

  void *ptr = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                   (off_t)slot_offset);
  close(fd);
  if (ptr == MAP_FAILED)
    throw std::runtime_error("mmap slot: " + std::string(strerror(errno)));

  RawHandle h{};
  h.backend_id = IPC_BACKEND_ID_POSIX_SHM;
  PosixShmHandle ph{};
  strncpy(ph.shm_name, shm_name, sizeof(ph.shm_name) - 1);
  ph.slot_offset = (uint64_t)slot_offset;
  ph.slot_bytes = (uint64_t)bytes;
  memcpy(h.data, &ph, sizeof(ph));
  return {ptr, h};
};

RawHandle PosixShmBackend::export_handle(void * /*ptr*/) {
  // not used directly — use alloc_named instead
  throw std::runtime_error("PosixShmBackend: use alloc_named");
};

void *PosixShmBackend::import_handle(const RawHandle &h) {
  if (h.backend_id != IPC_BACKEND_ID_POSIX_SHM)
    throw std::runtime_error("PosixShmBackend::import_handle: wrong backend_id " +
                             std::to_string(h.backend_id));
  PosixShmHandle ph{};
  memcpy(&ph, h.data, sizeof(ph));

  int fd = shm_open(ph.shm_name, O_RDWR, 0666);
  if (fd < 0)
    throw std::runtime_error(std::string("shm_open import: ") + ph.shm_name +
                             ": " + strerror(errno));

  void *ptr = mmap(nullptr, (size_t)ph.slot_bytes, PROT_READ | PROT_WRITE,
                   MAP_SHARED, fd, (off_t)ph.slot_offset);
  close(fd);
  if (ptr == MAP_FAILED)
    throw std::runtime_error("mmap import: " + std::string(strerror(errno)));
  return ptr;
};

void PosixShmBackend::close_handle(void *mapped_ptr, size_t bytes) {
  if (mapped_ptr)
    munmap(mapped_ptr, bytes);
};

}; // namespace sprint::ipc