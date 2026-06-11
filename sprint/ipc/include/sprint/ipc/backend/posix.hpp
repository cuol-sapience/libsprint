#ifndef SPRINT_IPC_BACKEND_POSIX_HPP_
#define SPRINT_IPC_BACKEND_POSIX_HPP_

#include "sprint/ipc/backend/common.hpp"

#include <cstdint>
#include <string>
#include <utility>

namespace sprint::ipc {

struct PosixShmHandle {
  char shm_name[64]; // "<base>_data"
  uint64_t slot_offset; // page-aligned byte offset of this slot within the shm
  uint64_t slot_bytes;  // raw slot size (used for mmap length and munmap)
};
static_assert(sizeof(PosixShmHandle) <= IPC_HANDLE_BYTES, "");

// TODO: gai here down in impl

struct PosixShmBackend {
  static constexpr uint32_t backend_id = IPC_BACKEND_ID_POSIX_SHM;
  static constexpr bool uses_named_alloc = true;

  static void *alloc(size_t bytes);
  static void free_ptr(void *ptr, size_t bytes = 0);
  // slot_index==0 creates the shm file (ring_size * page_align(bytes));
  // subsequent slots open the existing file and map at their page-aligned offset.
  static std::pair<void *, RawHandle>
  alloc_named(const std::string &base, uint32_t slot_index, uint32_t ring_size,
              size_t bytes);
  static RawHandle export_handle(void * /*ptr*/);
  static void *import_handle(const RawHandle &h);
  static void close_handle(void *mapped_ptr, size_t bytes = 0);

  static void sync(void * /*stream*/) { /* no-op for CPU */ }

  static constexpr const char *name() { return "posix_shm"; }
};

}; // namespace sprint::ipc

#endif /* SPRINT_IPC_BACKEND_POSIX_HPP_ */