#ifndef SPRINT_IPC_PRODUCER_HPP_
#define SPRINT_IPC_PRODUCER_HPP_

// todo: gai

#include "sprint/ipc/backend/selector.hpp"
#include "sprint/ipc/ipc.hpp"
#include "sprint/ipc/payload/common.hpp"
#include <cassert>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace sprint::ipc {

template <typename T, typename Backend = DefaultBackend> class IpcProducer {
public:
  using Traits = TypeTraits<T>;

  /**
   * @param shm_name        e.g. "/pcl_lidar0" or "/imu0"
   * @param max_elements    slot capacity; defaults to
   * Traits::max_elements
   * @param ring_size       number of slots; defaults to
   * Traits::ring_size must be power of 2, 2–SPRINT_IPC_MAX_RING_SIZE
   * @param policy          OVERWRITE_OLDEST or STALL_PER_CONSUMER
   * @param stall_timeout_ns  STALL_PER_CONSUMER timeout before dropping lagged
   * consumers
   */
  explicit IpcProducer(
      const std::string &shm_name,
      uint32_t max_elements = Traits::max_elements,
      uint32_t ring_size = Traits::ring_size,
      OverwritePolicy policy = Traits::policy,
      uint64_t stall_timeout_ns = 50'000'000ULL)
      : shm_name_(shm_name), max_elements_(max_elements), ring_size_(ring_size),
        policy_(policy), stall_timeout_ns_(stall_timeout_ns) {
    ipc_validate_ring_size(ring_size_, Traits::name());
    open_shm();
    alloc_ring();
    // RELEASE store signals consumers that all handles are written and safe to import.
    __atomic_store_n(&header_->write_seq, 1ULL, __ATOMIC_RELEASE);
  }

  ~IpcProducer() { destroy(); }

  IpcProducer(const IpcProducer &) = delete;
  IpcProducer &operator=(const IpcProducer &) = delete;

  // ── buffer access ─────────────────────────────────────────────────────

  /**
   * Pointer to the next slot's element buffer.
   * For CudaBackend: device pointer — fill via kernel or cudaMemcpy.
   * For PosixShmBackend: regular host pointer — write directly.
   *
   * Always call immediately before filling; slot rotates each publish().
   */
  T *next_slot_ptr() const {
    uint32_t slot = (uint32_t)(header_->write_seq % ring_size_);
    return ring_ptrs_[slot];
  }

  uint32_t max_elements() const { return max_elements_; }
  uint32_t ring_size() const { return ring_size_; }

  // ── publish ───────────────────────────────────────────────────────────

  /**
   * Commit next_slot_ptr() as a new frame.
   *
   * @param num_elements  valid elements in next_slot_ptr() (≤ max_elements)
   * @param stream        CudaBackend: cudaStream_t cast to void*; else ignored
   * @return number of consumers marked SLOW_DROPPED this call (0 = ok)
   */
  uint32_t publish(uint32_t num_elements, void *stream = nullptr) {
    // Per-type validation: range check, fixed-count enforcement, etc.
    // Throws std::runtime_error on violation (never silently corrupts).
    Traits::validate(num_elements, max_elements_);

    Backend::sync(stream);

    ShmHeader *h = header_;
    uint32_t dropped = 0;

    pthread_mutex_lock(&h->write_mutex);

    if (policy_ == OverwritePolicy::STALL_PER_CONSUMER) {
      struct timespec dl = ipc_deadline(stall_timeout_ns_);
      while (ipc_count_lagged(h) > 0) {
        int rc = pthread_cond_timedwait(&h->slot_free_cv, &h->write_mutex, &dl);
        if (rc == ETIMEDOUT) {
          dropped = ipc_drop_lagged(h);
          break;
        }
      }
    }

    uint32_t slot = (uint32_t)(h->write_seq % h->ring_size);
    SlotHeader &s = h->slots[slot];
    s.num_elements = num_elements;
    s.timestamp_ns = ipc_now_ns();
    __atomic_store_n(&s.seq, h->write_seq, __ATOMIC_RELEASE);

    uint64_t new_seq = h->write_seq++;

    for (uint32_t i = 0; i < h->num_consumers; ++i) {
      ConsumerState &c = h->consumers[i];
      if (c.flags & SPRINT_CONSUMER_DEAD)
        continue;
      if (c.wait_mode == WaitMode::ANY_NEW || c.read_seq + 1 == new_seq)
        pthread_cond_signal(&c.cv);
    }

    pthread_mutex_unlock(&h->write_mutex);
    return dropped;
  }

  // ── diagnostics ───────────────────────────────────────────────────────

  void print_stats(std::ostream &os = std::cout) const {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);
    os << "[producer:" << Traits::type_name() << ":" << Backend::name() << "] "
       << "write_seq=" << h->write_seq << " consumers=" << h->num_consumers
       << "\n";
    for (uint32_t i = 0; i < h->num_consumers; ++i) {
      const ConsumerState &c = h->consumers[i];
      os << "  [" << i << "] read_seq=" << c.read_seq
         << " behind=" << (int64_t)(h->write_seq - c.read_seq - 1)
         << " flags=0x" << std::hex << c.flags << std::dec << "\n";
    }
    pthread_mutex_unlock(&h->write_mutex);
  }

private:
  // ── /dev/shm header segment ───────────────────────────────────────────

  void open_shm() {
    shm_unlink(shm_name_.c_str());

    int fd = shm_open(shm_name_.c_str(), O_CREAT | O_RDWR, 0666);
    if (fd < 0)
      throw std::runtime_error("shm_open: " + std::string(strerror(errno)));

    shm_size_ = sizeof(ShmHeader);
    if (ftruncate(fd, (off_t)shm_size_) < 0)
      throw std::runtime_error("ftruncate: " + std::string(strerror(errno)));

    header_ = (ShmHeader *)mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE,
                                MAP_SHARED, fd, 0);
    close(fd);
    if (header_ == MAP_FAILED)
      throw std::runtime_error("mmap: " + std::string(strerror(errno)));

    memset(header_, 0, shm_size_);

    pthread_mutexattr_t ma;
    pthread_mutexattr_init(&ma);
    pthread_mutexattr_setpshared(&ma, PTHREAD_PROCESS_SHARED);
    pthread_mutexattr_setrobust(&ma, PTHREAD_MUTEX_ROBUST);
    pthread_mutex_init(&header_->write_mutex, &ma);
    pthread_mutexattr_destroy(&ma);

    pthread_condattr_t ca;
    pthread_condattr_init(&ca);
    pthread_condattr_setpshared(&ca, PTHREAD_PROCESS_SHARED);
    pthread_condattr_setclock(&ca, CLOCK_REALTIME);
    pthread_cond_init(&header_->slot_free_cv, &ca);
    for (uint32_t i = 0; i < IPC_GLOBAL_MAX_CONSUMERS; ++i)
      pthread_cond_init(&header_->consumers[i].cv, &ca);
    pthread_condattr_destroy(&ca);

    header_->magic = IPC_MAGIC;
    header_->version = IPC_VERSION;
    header_->max_elements = max_elements_;
    header_->element_size = sizeof(T);
    header_->ring_size = ring_size_;
    header_->backend_id = Backend::backend_id;
    header_->type_id = Traits::id;
    // write_seq stays 0 until alloc_ring() completes; consumers spin on it.
  }

  // ── ring allocation ───────────────────────────────────────────────────

  void alloc_ring() {
    size_t slot_bytes = (size_t)max_elements_ * sizeof(T);
    alloc_ring_impl(slot_bytes, std::bool_constant<Backend::uses_named_alloc>{});
  }

  void alloc_ring_impl(size_t slot_bytes, std::true_type /*named alloc*/) {
    for (uint32_t i = 0; i < ring_size_; ++i) {
      auto [ptr, handle] =
          Backend::alloc_named(shm_name_, i, ring_size_, slot_bytes);
      ring_ptrs_[i] = reinterpret_cast<T *>(ptr);
      ring_slot_bytes_[i] = slot_bytes;
      header_->slots[i].handle = handle;
      header_->slots[i].seq = 0;
    }
  }

  void alloc_ring_impl(size_t slot_bytes, std::false_type /*anon alloc*/) {
    for (uint32_t i = 0; i < ring_size_; ++i) {
      void *ptr = Backend::alloc(slot_bytes);
      ring_ptrs_[i] = reinterpret_cast<T *>(ptr);
      ring_slot_bytes_[i] = slot_bytes;
      header_->slots[i].handle = Backend::export_handle(ptr);
      header_->slots[i].seq = 0;
    }
  }

  // ── teardown ──────────────────────────────────────────────────────────

  void destroy() {
    for (uint32_t i = 0; i < ring_size_; ++i) {
      if (!ring_ptrs_[i])
        continue;
      Backend::free_ptr(ring_ptrs_[i], ring_slot_bytes_[i]);
      ring_ptrs_[i] = nullptr;
    }
    if (header_ && header_ != MAP_FAILED) {
      pthread_mutex_destroy(&header_->write_mutex);
      pthread_cond_destroy(&header_->slot_free_cv);
      for (uint32_t i = 0; i < IPC_GLOBAL_MAX_CONSUMERS; ++i)
        pthread_cond_destroy(&header_->consumers[i].cv);
      munmap(header_, shm_size_);
      header_ = nullptr;
    }
    shm_unlink(shm_name_.c_str());
    unlink_posix_slots(std::bool_constant<Backend::uses_named_alloc>{});
  }

  void unlink_posix_slots(std::true_type) {
    char name[64];
    snprintf(name, sizeof(name), "%.56s_data", shm_name_.c_str());
    shm_unlink(name);
  }
  void unlink_posix_slots(std::false_type) {}

  std::string shm_name_;
  uint32_t max_elements_;
  uint32_t ring_size_;
  OverwritePolicy policy_;
  uint64_t stall_timeout_ns_;

  ShmHeader *header_ = nullptr;
  size_t shm_size_ = 0;
  T *ring_ptrs_[IPC_GLOBAL_MAX_RING_SIZE] = {};
  size_t ring_slot_bytes_[IPC_GLOBAL_MAX_RING_SIZE] = {};
};
}; // namespace sprint::ipc

#endif /**  SPRINT_IPC_PRODUCER_HPP_ */