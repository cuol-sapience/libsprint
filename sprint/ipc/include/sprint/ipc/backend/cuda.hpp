#ifndef SPRINT_IPC_BACKEND_CUDA_HPP_
#define SPRINT_IPC_BACKEND_CUDA_HPP_

// TODO: gai here in impl
#if defined(SPRINT_IPC_ENABLE_CUDA)

#include "sprint/ipc/backend/common.hpp"

#include <cstring>
#include <cuda_runtime.h>
#include <stdexcept>

namespace sprint::ipc {

inline void pcl_cuda_check(cudaError_t e, const char *msg) {
  if (e != cudaSuccess)
    throw std::runtime_error(std::string(msg) + ": " + cudaGetErrorString(e));
}

// Probe whether cudaIpcGetMemHandle works on device 0 (fails on Jetson iGPU).
static bool cuda_supports_ipc() {
  void *ptr = nullptr;
  if (cudaMalloc(&ptr, 4096) != cudaSuccess)
    return false;
  cudaIpcMemHandle_t handle;
  bool ok = (cudaIpcGetMemHandle(&handle, ptr) == cudaSuccess);
  cudaFree(ptr);
  return ok;
};

// Check whether the device supports mapping pinned host memory into CUDA's VA
// (required for cudaHostRegister with cudaHostRegisterMapped).
static bool cuda_supports_zero_copy() {
  int val = 0;
  return cudaDeviceGetAttribute(&val, cudaDevAttrCanMapHostMemory, 0) ==
             cudaSuccess &&
         val != 0;
};

struct CudaBackend {
  static constexpr uint32_t backend_id = IPC_BACKEND_ID_CUDA;
  static constexpr bool uses_named_alloc = false;

  using StreamT = cudaStream_t; // pass as void* in sync()

  static void *alloc(size_t bytes) {
    void *ptr = nullptr;
    pcl_cuda_check(cudaMalloc(&ptr, bytes), "cudaMalloc");
    return ptr;
  }

  static void free_ptr(void *ptr, size_t /*bytes*/ = 0) {
    if (ptr)
      cudaFree(ptr);
  }

  static RawHandle export_handle(void *ptr) {
    if (!ptr)
      throw std::runtime_error("CudaBackend::export_handle: null pointer");
    RawHandle h{};
    h.backend_id = IPC_BACKEND_ID_CUDA;
    static_assert(sizeof(cudaIpcMemHandle_t) <= IPC_HANDLE_BYTES, "");
    cudaIpcMemHandle_t ipc{};
    pcl_cuda_check(cudaIpcGetMemHandle(&ipc, ptr), "cudaIpcGetMemHandle");
    memcpy(h.data, &ipc, sizeof(ipc));
    return h;
  }

  static void *import_handle(const RawHandle &h) {
    if (h.backend_id != IPC_BACKEND_ID_CUDA)
      throw std::runtime_error("CudaBackend::import_handle: wrong backend_id " +
                               std::to_string(h.backend_id));
    cudaIpcMemHandle_t ipc{};
    memcpy(&ipc, h.data, sizeof(ipc));
    void *ptr = nullptr;
    pcl_cuda_check(
        cudaIpcOpenMemHandle(&ptr, ipc, cudaIpcMemLazyEnablePeerAccess),
        "cudaIpcOpenMemHandle");
    return ptr;
  }

  static void close_handle(void *mapped_ptr, size_t /*bytes*/ = 0) {
    if (mapped_ptr)
      cudaIpcCloseMemHandle(mapped_ptr);
  }

  static void sync(void *stream) {
    if (stream)
      cudaStreamSynchronize(static_cast<cudaStream_t>(stream));
    else
      cudaDeviceSynchronize();
  }

  static constexpr const char *name() { return "cuda"; }
};

}; // namespace sprint::ipc

#endif

#endif /* SPRINT_IPC_BACKEND_CUDA_HPP_ */