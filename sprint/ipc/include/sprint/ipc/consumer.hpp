#ifndef SPRINT_IPC_CONSUMER_HPP_
#define SPRINT_IPC_CONSUMER_HPP_

#include "sprint/ipc/backend/selector.hpp"
#include "sprint/ipc/ipc.hpp"
#include "sprint/ipc/payload/common.hpp"

#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// todo: gai

namespace sprint::ipc {

template <typename T, typename Backend = DefaultBackend> class IpcConsumer {
public:
  using Traits = TypeTraits<T>;

  explicit IpcConsumer(const std::string &shm_name) : shm_name_(shm_name) {
    open_shm();
    check_header();
    map_ring();
    consumer_id_ = register_self();
  }

  ~IpcConsumer() { destroy(); }

  IpcConsumer(const IpcConsumer &) = delete;
  IpcConsumer &operator=(const IpcConsumer &) = delete;

  // ── frame descriptor ─────────────────────────────────────────────────

  struct Frame {
    uint64_t seq;
    uint32_t num_elements; // valid elements in ptr[]
    uint64_t timestamp_ns;
    T *ptr; // device ptr (CUDA) or host ptr (POSIX)
    uint32_t slot;
    bool was_dropped;

    uint32_t _max_elements; // from ShmHeader; used by count()
    uint32_t count() const { return num_elements; }
  };

  // ── accessors ────────────────────────────────────────────────────────

  uint32_t consumer_id() const { return consumer_id_; }
  uint64_t read_seq() const { return read_seq_local_; }
  const char *type_name() const { return Traits::type_name(); }

  uint32_t frames_available() const {
    pthread_mutex_lock(&header_->write_mutex);
    uint64_t ws = header_->write_seq;
    uint32_t rs = header_->ring_size;
    pthread_mutex_unlock(&header_->write_mutex);
    uint64_t behind = ws - read_seq_local_ - 1;
    return (uint32_t)std::min(behind, (uint64_t)rs);
  }

  // ── receive: blocking ─────────────────────────────────────────────────

  /**
   * Block until the next sequential frame is available.
   * @param timeout_ns  0 = wait forever
   */
  std::optional<Frame> wait_next(uint64_t timeout_ns = 0) {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);

    uint64_t want = read_seq_local_ + 1;
    struct timespec dl{};
    if (timeout_ns > 0)
      dl = ipc_deadline(timeout_ns);

    h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;

    while (!slot_ready(h, want)) {
      int rc = (timeout_ns == 0)
                   ? (pthread_cond_wait(&h->consumers[consumer_id_].cv,
                                        &h->write_mutex),
                      0)
                   : pthread_cond_timedwait(&h->consumers[consumer_id_].cv,
                                            &h->write_mutex, &dl);
      if (rc == ETIMEDOUT) {
        h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;
        pthread_mutex_unlock(&h->write_mutex);
        return std::nullopt;
      }
    }

    h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;
    Frame f = make_frame(h, want);
    advance(h, want);
    pthread_mutex_unlock(&h->write_mutex);
    return f;
  }

  // ── receive: non-blocking ─────────────────────────────────────────────

  std::optional<Frame> try_next() {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);
    std::optional<Frame> result;
    uint64_t want = read_seq_local_ + 1;
    if (slot_ready(h, want)) {
      result = make_frame(h, want);
      advance(h, want);
    }
    pthread_mutex_unlock(&h->write_mutex);
    return result;
  }

  // ── receive: skip to latest ───────────────────────────────────────────

  /**
   * Skip any queued backlog and return the most recent frame.
   * Good for visualisers and status monitors.
   */
  std::optional<Frame> catch_up(uint64_t timeout_ns = 0) {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);

    struct timespec dl{};
    if (timeout_ns > 0)
      dl = ipc_deadline(timeout_ns);

    h->consumers[consumer_id_].wait_mode = WaitMode::ANY_NEW;

    while (h->write_seq <= 1) {
      int rc = (timeout_ns == 0)
                   ? (pthread_cond_wait(&h->consumers[consumer_id_].cv,
                                        &h->write_mutex),
                      0)
                   : pthread_cond_timedwait(&h->consumers[consumer_id_].cv,
                                            &h->write_mutex, &dl);
      if (rc == ETIMEDOUT) {
        h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;
        pthread_mutex_unlock(&h->write_mutex);
        return std::nullopt;
      }
    }

    h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;

    uint64_t latest = h->write_seq - 1;
    while (!slot_ready(h, latest) && latest > read_seq_local_ + 1)
      --latest;
    if (!slot_ready(h, latest)) {
      pthread_mutex_unlock(&h->write_mutex);
      return std::nullopt;
    }

    Frame f = make_frame(h, latest);
    advance(h, latest);
    pthread_mutex_unlock(&h->write_mutex);
    return f;
  }

  // ── callback style ────────────────────────────────────────────────────

  bool process_next(std::function<void(const Frame &)> cb,
                    uint64_t timeout_ns = 0) {
    auto f = wait_next(timeout_ns);
    if (!f)
      return false;
    cb(*f);
    return true;
  }

  // Like wait_next() but invokes fn(frame) BEFORE advancing read_seq.
  // Use this when the caller needs to finish accessing slot memory (e.g.
  // a cudaMemcpy D→H) before the producer is allowed to reclaim the slot.
  template <typename F>
  std::optional<Frame> wait_next_and_consume(F &&fn,
                                             uint64_t timeout_ns = 0) {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);

    uint64_t want = read_seq_local_ + 1;
    struct timespec dl{};
    if (timeout_ns > 0)
      dl = ipc_deadline(timeout_ns);

    h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;

    while (!slot_ready(h, want)) {
      int rc = (timeout_ns == 0)
                   ? (pthread_cond_wait(&h->consumers[consumer_id_].cv,
                                        &h->write_mutex),
                      0)
                   : pthread_cond_timedwait(&h->consumers[consumer_id_].cv,
                                            &h->write_mutex, &dl);
      if (rc == ETIMEDOUT) {
        h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;
        pthread_mutex_unlock(&h->write_mutex);
        return std::nullopt;
      }
    }

    h->consumers[consumer_id_].wait_mode = WaitMode::SEQUENTIAL;
    Frame f = make_frame(h, want);
    fn(f);        // consume slot data before signalling the producer it's free
    advance(h, want);
    pthread_mutex_unlock(&h->write_mutex);
    return f;
  }

  // ── unregister ────────────────────────────────────────────────────────

  void unregister() {
    if (consumer_id_ == UINT32_MAX)
      return;
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);
    h->consumers[consumer_id_].flags |= SPRINT_CONSUMER_DEAD;
    h->consumers[consumer_id_].flags &= ~SPRINT_CONSUMER_ACTIVE;
    pthread_cond_signal(&h->slot_free_cv);
    pthread_mutex_unlock(&h->write_mutex);
    consumer_id_ = UINT32_MAX;
  }

private:
  // ── slot_ready ────────────────────────────────────────────────────────

  static bool slot_ready(const ShmHeader *h, uint64_t seq) {
    if (seq == 0 || seq >= h->write_seq)
      return false;
    uint32_t slot = (uint32_t)(seq % h->ring_size);
    uint64_t written = __atomic_load_n(&h->slots[slot].seq, __ATOMIC_ACQUIRE);
    return written == seq;
  }

  // ── make_frame ────────────────────────────────────────────────────────

  Frame make_frame(const ShmHeader *h, uint64_t seq) const {
    uint32_t slot = (uint32_t)(seq % h->ring_size);
    bool dropped =
        (h->consumers[consumer_id_].flags & SPRINT_CONSUMER_SLOW_DROPPED) != 0;
    return Frame{seq,
                 h->slots[slot].num_elements,
                 h->slots[slot].timestamp_ns,
                 ring_ptrs_[slot],
                 slot,
                 dropped};
  }

  // ── advance ───────────────────────────────────────────────────────────

  void advance(ShmHeader *h, uint64_t seq) {
    h->consumers[consumer_id_].read_seq = seq;
    h->consumers[consumer_id_].flags &= ~SPRINT_CONSUMER_SLOW_DROPPED;
    read_seq_local_ = seq;
    pthread_cond_signal(&h->slot_free_cv);
  }

  // ── open /dev/shm ─────────────────────────────────────────────────────

  void open_shm() {
    int fd = shm_open(shm_name_.c_str(), O_RDWR, 0666);
    if (fd < 0)
      throw std::runtime_error("shm_open (producer running?): " +
                               std::string(strerror(errno)));
    shm_size_ = sizeof(ShmHeader);
    header_ = (ShmHeader *)mmap(nullptr, shm_size_, PROT_READ | PROT_WRITE,
                                MAP_SHARED, fd, 0);
    close(fd);
    if (header_ == MAP_FAILED)
      throw std::runtime_error("mmap: " + std::string(strerror(errno)));
  }

  // ── header validation ─────────────────────────────────────────────────
  //
  // Three checks: magic/version (corrupt/stale segment), type_id (wrong
  // data type), backend_id (mismatched memory backend).  All produce
  // descriptive errors rather than silent memory misinterpretation.

  void check_header() const {
    if (header_->magic != IPC_MAGIC)
      throw std::runtime_error("magic mismatch — stale or wrong segment");
    if (header_->version != IPC_VERSION)
      throw std::runtime_error(
          "version mismatch: segment=" + std::to_string(header_->version) +
          " consumer=" + std::to_string(IPC_VERSION));

    if (header_->type_id != Traits::id)
      throw std::runtime_error(
          std::string("type mismatch: segment has type_id=0x") +
          to_hex(header_->type_id) + " but consumer expects " + Traits::name() +
          " (0x" + to_hex(Traits::id) + ")");

    if (header_->element_size != sizeof(T))
      throw std::runtime_error(std::string("element_size mismatch: segment=") +
                               std::to_string(header_->element_size) +
                               " sizeof(T)=" + std::to_string(sizeof(T)));

    if (header_->backend_id != Backend::backend_id)
      throw std::runtime_error(
          std::string("backend mismatch: segment_id=") +
          std::to_string(header_->backend_id) +
          " consumer=" + Backend::name() +
          " (id=" + std::to_string(Backend::backend_id) + ")");
  }

  static std::string to_hex(uint32_t v) {
    char buf[9];
    snprintf(buf, sizeof(buf), "%08X", v);
    return buf;
  }

  // ── map ring ──────────────────────────────────────────────────────────

  void map_ring() {
    // Wait until the producer has finished alloc_ring() and written all handles.
    // The producer does an ATOMIC_RELEASE store of write_seq=1 after alloc_ring();
    // we pair with an ATOMIC_ACQUIRE load so all handle writes are visible.
    while (__atomic_load_n(&header_->write_seq, __ATOMIC_ACQUIRE) == 0)
      usleep(1000); // 1 ms; CUDA context init can take hundreds of ms

    for (uint32_t i = 0; i < header_->ring_size; ++i) {
      void *ptr = Backend::import_handle(header_->slots[i].handle);
      ring_ptrs_[i] = reinterpret_cast<T *>(ptr);
      ring_slot_bytes_[i] = (size_t)header_->max_elements * sizeof(T);
    }
  }

  // ── register ──────────────────────────────────────────────────────────

  uint32_t register_self() {
    ShmHeader *h = header_;
    pthread_mutex_lock(&h->write_mutex);

    uint32_t id = UINT32_MAX;
    for (uint32_t i = 0; i < h->num_consumers; ++i) {
      if (h->consumers[i].flags & SPRINT_CONSUMER_DEAD) {
        id = i;
        break;
      }
    }
    if (id == UINT32_MAX) {
      if (h->num_consumers >= IPC_GLOBAL_MAX_CONSUMERS) {
        pthread_mutex_unlock(&h->write_mutex);
        throw std::runtime_error("Consumer slots full");
      }
      id = h->num_consumers++;
    }

    ConsumerState &c = h->consumers[id];
    c.read_seq = (h->write_seq > 0) ? h->write_seq - 1 : 0;
    c.flags = SPRINT_CONSUMER_ACTIVE;
    c.wait_mode = WaitMode::SEQUENTIAL;
    read_seq_local_ = c.read_seq;
    pthread_mutex_unlock(&h->write_mutex);
    return id;
  }

  // ── teardown ──────────────────────────────────────────────────────────

  void destroy() {
    unregister();
    uint32_t rs = header_ ? header_->ring_size : IPC_GLOBAL_MAX_RING_SIZE;
    for (uint32_t i = 0; i < rs; ++i) {
      if (!ring_ptrs_[i])
        continue;
      Backend::close_handle(ring_ptrs_[i], ring_slot_bytes_[i]);
      ring_ptrs_[i] = nullptr;
    }
    if (header_ && header_ != MAP_FAILED) {
      munmap(header_, shm_size_);
      header_ = nullptr;
    }
  }

  std::string shm_name_;
  uint32_t consumer_id_ = UINT32_MAX;
  uint64_t read_seq_local_ = 0;

  ShmHeader *header_ = nullptr;
  size_t shm_size_ = 0;
  T *ring_ptrs_[IPC_GLOBAL_MAX_RING_SIZE] = {};
  size_t ring_slot_bytes_[IPC_GLOBAL_MAX_RING_SIZE] = {};
};

}; // namespace sprint::ipc

#endif /**  SPRINT_IPC_CONSUMER_HPP_ */