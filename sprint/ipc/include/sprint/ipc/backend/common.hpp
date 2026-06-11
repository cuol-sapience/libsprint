#ifndef SPRINT_IPC_BACKEND_COMMON_HPP_
#define SPRINT_IPC_BACKEND_COMMON_HPP_

#include <cstddef>
#include <cstdint>

// Assume posix always available
// #include "sprint/ipc/backend/posix.hpp"

namespace sprint::ipc {

static constexpr uint32_t IPC_GLOBAL_MAX_CONSUMERS = 64;
static constexpr uint32_t IPC_GLOBAL_MAX_RING_SIZE = 256;

// cudaIpcMemHandle_t is 64 bytes; we reserve a bit more for future backends.
static constexpr size_t IPC_HANDLE_BYTES = 128;

struct RawHandle {
  unsigned char data[IPC_HANDLE_BYTES];
  uint32_t backend_id; // IPC_BACKEND_ID_*
  uint32_t _pad;
};
static_assert(sizeof(RawHandle) == IPC_HANDLE_BYTES + 8);

static constexpr uint32_t IPC_BACKEND_ID_POSIX_SHM = 0x01;
static constexpr uint32_t IPC_BACKEND_ID_CUDA = 0x02;
static constexpr uint32_t IPC_BACKEND_ID_ZERO_COPY = 0x03;

}; // namespace sprint::ipc

#endif /* SPRINT_IPC_BACKEND_COMMON_HPP_ */