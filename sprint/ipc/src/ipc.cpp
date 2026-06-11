#include "sprint/ipc/ipc.hpp"
#include <string>

namespace sprint::ipc {

uint64_t ipc_now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1'000'000'000ULL + (uint64_t)ts.tv_nsec;
};

struct timespec ipc_deadline(uint64_t timeout_ns) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += (time_t)(timeout_ns / 1'000'000'000ULL);
  ts.tv_nsec += (long)(timeout_ns % 1'000'000'000ULL);
  if (ts.tv_nsec >= 1'000'000'000L) {
    ++ts.tv_sec;
    ts.tv_nsec -= 1'000'000'000L;
  }
  return ts;
};

uint32_t ipc_count_lagged(const ShmHeader *h) {
  uint32_t n = 0;
  for (uint32_t i = 0; i < h->num_consumers; ++i) {
    const ConsumerState &c = h->consumers[i];
    if (c.flags & SPRINT_CONSUMER_DEAD)
      continue;
    uint64_t behind = h->write_seq - c.read_seq;
    if (behind >= h->ring_size)
      ++n;
  }
  return n;
};

uint32_t ipc_drop_lagged(ShmHeader *h) {
  uint32_t n = 0;
  for (uint32_t i = 0; i < h->num_consumers; ++i) {
    ConsumerState &c = h->consumers[i];
    if (c.flags & SPRINT_CONSUMER_DEAD)
      continue;
    if (h->write_seq - c.read_seq >= h->ring_size) {
      c.flags |= SPRINT_CONSUMER_SLOW_DROPPED;
      c.read_seq = h->write_seq - h->ring_size + 1;
      ++n;
    }
  }
  return n;
};

}; // namespace sprint::ipc