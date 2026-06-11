#ifndef SPRINT_IPC_BACKEND_ZERO_COPY_HPP_
#define SPRINT_IPC_BACKEND_ZERO_COPY_HPP_

#if defined(SPRINT_IPC_ENABLE_CUDA)

#include "sprint/ipc/backend/cuda.hpp"
#include "sprint/ipc/backend/posix.hpp"

#include <cuda_runtime.h>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

namespace sprint::ipc {

// Cross-process zero-copy backend for systems with unified host/device memory
// (e.g. Jetson Orin iGPU). Uses POSIX shm for cross-process data sharing and
// cudaHostRegister to make it accessible from CUDA kernels without memcpy.
//
// On Jetson Orin the pointer returned by alloc_named/import_handle is valid
// for both host and device access (same VA). On discrete GPUs you would need
// cudaHostGetDevicePointer for kernel access, but IPC is unsupported there
// anyway — this backend targets the Jetson unified-memory case.
struct CudaZeroCopyBackend {
  static constexpr uint32_t backend_id = IPC_BACKEND_ID_ZERO_COPY;
  static constexpr bool uses_named_alloc = true;

  static std::pair<void *, RawHandle>
  alloc_named(const std::string &base, uint32_t slot_index, uint32_t ring_size,
              size_t bytes) {
    auto [ptr, handle] =
        PosixShmBackend::alloc_named(base, slot_index, ring_size, bytes);
    pcl_cuda_check(
        cudaHostRegister(ptr, bytes,
                         cudaHostRegisterMapped | cudaHostRegisterPortable),
        "cudaHostRegister");
    handle.backend_id = IPC_BACKEND_ID_ZERO_COPY;
    return {ptr, handle};
  }

  static void *import_handle(const RawHandle &h) {
    if (h.backend_id != IPC_BACKEND_ID_ZERO_COPY)
      throw std::runtime_error(
          "CudaZeroCopyBackend::import_handle: wrong backend_id " +
          std::to_string(h.backend_id));

    PosixShmHandle ph{};
    memcpy(&ph, h.data, sizeof(ph));

    int fd = shm_open(ph.shm_name, O_RDWR, 0666);
    if (fd < 0)
      throw std::runtime_error(std::string("shm_open: ") + ph.shm_name + ": " +
                               strerror(errno));
    void *ptr =
        mmap(nullptr, (size_t)ph.slot_bytes, PROT_READ | PROT_WRITE,
             MAP_SHARED, fd, (off_t)ph.slot_offset);
    close(fd);
    if (ptr == MAP_FAILED)
      throw std::runtime_error("mmap: " + std::string(strerror(errno)));

    pcl_cuda_check(
        cudaHostRegister(ptr, (size_t)ph.slot_bytes,
                         cudaHostRegisterMapped | cudaHostRegisterPortable),
        "cudaHostRegister (consumer)");
    return ptr;
  }

  static void free_ptr(void *ptr, size_t bytes) {
    if (!ptr)
      return;
    cudaHostUnregister(ptr);
    munmap(ptr, bytes);
  }

  static void close_handle(void *ptr, size_t bytes) {
    if (!ptr)
      return;
    cudaHostUnregister(ptr);
    munmap(ptr, bytes);
  }

  // alloc/export_handle not used — zero-copy always goes through alloc_named.
  static void *alloc(size_t) {
    throw std::runtime_error("CudaZeroCopyBackend: use alloc_named");
  }
  static RawHandle export_handle(void *) {
    throw std::runtime_error("CudaZeroCopyBackend: use alloc_named");
  }

  static void sync(void * /*stream*/) {}

  static constexpr const char *name() { return "cuda_zero_copy"; }
};

}; // namespace sprint::ipc

#endif // SPRINT_IPC_ENABLE_CUDA
#endif /* SPRINT_IPC_BACKEND_ZERO_COPY_HPP_ */
