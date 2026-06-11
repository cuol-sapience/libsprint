#ifndef SPRINT_IPC_IPC_HPP_
#define SPRINT_IPC_IPC_HPP_

#include "backend/common.hpp"

#include <cstdint>
#include <pthread.h>
#include <stdexcept>
#include <unistd.h>
namespace sprint::ipc {

static constexpr uint32_t IPC_MAGIC = 0x53504e54u; // "SPNT"
static constexpr uint32_t IPC_VERSION = 1u;

#pragma region Consumer flags

static constexpr uint32_t SPRINT_CONSUMER_ACTIVE = 0x01;
static constexpr uint32_t SPRINT_CONSUMER_SLOW_DROPPED = 0x02;
static constexpr uint32_t SPRINT_CONSUMER_DEAD = 0x04;

#pragma endregion

enum class OverwritePolicy {
  OVERWRITE_OLDEST,   // never stall; slow consumers silently lose frames
  STALL_PER_CONSUMER, // stall briefly for lagged consumers, then drop them
};

// TODO: gai
// ── per-consumer wait mode ────────────────────────────────────────────────
//
// Set by the consumer before blocking on its cv; read by the producer to
// decide which consumers to signal after each write.
//
//   SEQUENTIAL  consumer wants exactly read_seq+1 — signal only when that
//               specific slot is written.
//
//   ANY_NEW     catch_up() style — consumer wants any frame newer than what
//               it last read — signal on every new write.
//
// The producer signals c.cv when:
//   SEQUENTIAL:  new_seq == c.read_seq + 1
//   ANY_NEW:     always (new_seq > c.read_seq)
//
// The consumer always re-checks slot_ready() after waking (spurious wakeups
// are possible and wait_mode is set under write_mutex so there is no race).

enum class WaitMode : uint8_t {
  SEQUENTIAL = 0,
  ANY_NEW = 1,
};

#pragma region Structures

// TODO: add alignas
struct SlotHeader {
  uint64_t seq;          // which frame seq is in this slot (0 = empty)
  uint32_t num_elements; // valid elements written this frame
  uint32_t _pad0;
  uint64_t timestamp_ns;
  RawHandle handle; // backend-specific IPC handle (set at startup)
};

struct ConsumerState {
  uint64_t read_seq;  // last seq this consumer read (0 = not started)
  uint32_t flags;     // consumer flags this file
  WaitMode wait_mode; // SEQUENTIAL or ANY_NEW; set before sleeping
  uint8_t _pad[3];
  pthread_cond_t cv; // signalled individually by producer
};

struct ShmHeader {
  // identity
  uint32_t magic;
  uint32_t version;
  uint32_t max_elements; // points per slot (fixed at creation)
  uint32_t element_size; // sizeof()
  uint32_t ring_size;
  uint32_t backend_id;
  uint32_t type_id; // TypeTraits<T>::type_id — checked at attach
  uint32_t _pad0;

  // producer
  uint64_t write_seq; // starting at 1, id of next sequence to write

  // consumer registry
  uint32_t num_consumers;
  uint32_t _pad1;

  // ring buffer
  SlotHeader slots[IPC_GLOBAL_MAX_RING_SIZE];

  // consumers
  ConsumerState consumers[IPC_GLOBAL_MAX_CONSUMERS];

  // synchronisation
  pthread_mutex_t write_mutex;
  pthread_cond_t slot_free_cv; // producer stall path
};

#pragma endregion

#pragma region Utility functions

// TODO: gai from here down (the impls)

inline void ipc_validate_ring_size(uint32_t rs, std::string type_name) {
  if (rs < 2 || rs > IPC_GLOBAL_MAX_RING_SIZE)
    throw std::runtime_error(type_name + ": ring_size=" + std::to_string(rs) +
                             " out of range [2, " +
                             std::to_string(IPC_GLOBAL_MAX_RING_SIZE) + "]");
  if (rs & (rs - 1))
    throw std::runtime_error(type_name + ": ring_size=" + std::to_string(rs) +
                             " is not a power of 2");
};

// current time
uint64_t ipc_now_ns();

// deadline to respond (timeout from this instant)
struct timespec ipc_deadline(uint64_t timeout_ns);

// Count live consumers that would be lapped if producer advances now.
uint32_t ipc_count_lagged(const ShmHeader *h);

// Advance lagged consumers to the oldest safe slot, flag SLOW_DROPPED.
uint32_t ipc_drop_lagged(ShmHeader *h);

static void validate(uint32_t n, uint32_t max_elements);
static void validate(uint32_t n, uint32_t /*max_elem*/);

// ── shm size breakdown ────────────────────────────────────────────────────

struct ShmSizeInfo {
  size_t element_bytes;      // sizeof(T)
  size_t slot_bytes;         // max_elements * element_bytes
  size_t slot_bytes_aligned; // slot_bytes rounded up to OS page size (POSIX
                             // data segment)
  size_t ring_bytes;         // ring_size * slot_bytes_aligned
  size_t header_bytes;       // sizeof(ShmHeader) — control segment
  size_t total_bytes;        // header_bytes + ring_bytes
};

template <typename T>
ShmSizeInfo ipc_shm_size_info(uint32_t max_elements, uint32_t ring_size) {
  size_t elem = sizeof(T);
  size_t slot = (size_t)max_elements * elem;
  size_t page = (size_t)getpagesize();
  size_t slot_aligned = (slot + page - 1) & ~(page - 1);
  size_t ring = (size_t)ring_size * slot_aligned;
  size_t hdr = sizeof(ShmHeader);
  return {elem, slot, slot_aligned, ring, hdr, hdr + ring};
}

#pragma endregion

}; // namespace sprint::ipc

#endif /* SPRINT_IPC_IPC_HPP_ */