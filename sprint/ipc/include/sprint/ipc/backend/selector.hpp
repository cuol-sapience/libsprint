#ifndef SPRINT_IPC_BACKEND_SELECTOR_HPP_
#define SPRINT_IPC_BACKEND_SELECTOR_HPP_

// Assume posix always available
#include "sprint/ipc/backend/posix.hpp"

#if defined(SPRINT_IPC_ENABLE_CUDA)
#include "sprint/ipc/backend/cuda.hpp"
#include "sprint/ipc/backend/zero_copy.hpp"
#endif

namespace sprint::ipc {

// #if defined(SPRINT_IPC_ENABLE_CUDA)
// using DefaultBackend = CudaBackend;
// #else
using DefaultBackend = PosixShmBackend;
// #endif

}; // namespace sprint::ipc

#endif /* SPRINT_IPC_BACKEND_SELECTOR_HPP_ */