#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <random>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

#include "payload.hpp"

#ifdef SPRINT_IPC_ENABLE_CUDA
#include <cuda_runtime.h>
#endif

using namespace sprint::ipc;

static std::atomic<bool> is_running{true};
static void on_signal(int) { is_running = false; }

static const char *shm_name = "/sprint_demo0";

#pragma region Producer

void run_producer() {

  std::default_random_engine gen;
  std::uniform_real_distribution<float> distribution(0.0, 1.0);

  constexpr uint32_t num_elements = 1;
  DemoProducer<PosixShmBackend> prod(shm_name, num_elements);

  std::cout << "[demo_producer:" << prod.ring_size() << "-slot ring] started\n";

  uint64_t frame = 0;
  while (is_running) {
    auto t0 = std::chrono::steady_clock::now();

    prod.next_slot_ptr()->number = distribution(gen);

    uint32_t dropped = prod.publish(num_elements);

    if (dropped)
      std::cerr << "[demo_producer] frame=" << frame << " dropped " << dropped
                << " consumer(s)\n";

    auto rem = std::chrono::microseconds(50000) -
               (std::chrono::steady_clock::now() - t0);
    if (rem > std::chrono::microseconds(0))
      std::this_thread::sleep_for(rem);
    ++frame;
  }
};

#pragma endregion

#pragma region Stats

static std::string fmt_bytes(size_t n) {
  char buf[32];
  if (n >= 1024 * 1024)
    snprintf(buf, sizeof(buf), "%.2f MiB", n / (1024.0 * 1024.0));
  else if (n >= 1024)
    snprintf(buf, sizeof(buf), "%.2f KiB", n / 1024.0);
  else
    snprintf(buf, sizeof(buf), "%zu B", n);
  return buf;
}

void run_stats() {
  int fd = shm_open(shm_name, O_RDONLY, 0);
  if (fd < 0) {
    std::cerr << "shm_open: " << strerror(errno)
              << " (is the producer running?)\n";
    return;
  }

  auto *h = static_cast<ShmHeader *>(
      mmap(nullptr, sizeof(ShmHeader), PROT_READ, MAP_SHARED, fd, 0));
  close(fd);
  if (h == MAP_FAILED) {
    std::cerr << "mmap: " << strerror(errno) << "\n";
    return;
  }

  if (h->magic != IPC_MAGIC) {
    std::cerr << "bad magic — stale or wrong segment\n";
    munmap(h, sizeof(ShmHeader));
    return;
  }

  while (is_running) {
    auto sz = ipc_shm_size_info<DemoPayload>(h->max_elements, h->ring_size);
    uint64_t write_seq = h->write_seq;
    uint32_t num_consumers = h->num_consumers;

    std::cout << "\033[2J\033[H\n\n"; // clear + home

    std::cout << "=== " << shm_name << "  [" << TypeTraits<DemoPayload>::name()
              << "  ring=" << h->ring_size
              << "  max_elements=" << h->max_elements << "] ===\n\n";

    std::cout << "Memory\n"
              << "  element          " << fmt_bytes(sz.element_bytes) << "\n"
              << "  slot             " << fmt_bytes(sz.slot_bytes) << "  ("
              << h->max_elements << " element"
              << (h->max_elements != 1 ? "s" : "") << ")\n"
              << "  slot (aligned)   " << fmt_bytes(sz.slot_bytes_aligned)
              << "\n"
              << "  ring             " << fmt_bytes(sz.ring_bytes) << "  ("
              << h->ring_size << " slots)\n"
              << "  header           " << fmt_bytes(sz.header_bytes) << "\n"
              << "  ───────────────────────────\n"
              << "  total            " << fmt_bytes(sz.total_bytes) << "\n\n";

    std::cout << "Ring  write_seq=" << write_seq
              << "  consumers=" << num_consumers << "/"
              << IPC_GLOBAL_MAX_CONSUMERS << "\n\n";

    if (num_consumers == 0) {
      std::cout << "  (no consumers)\n";
    } else {
      std::cout << "  ID   read_seq      behind   status\n"
                << "  ──   ────────      ──────   ──────\n";
      for (uint32_t i = 0; i < num_consumers; ++i) {
        const ConsumerState &c = h->consumers[i];
        int64_t behind = (int64_t)(write_seq - c.read_seq - 1);

        const char *status;
        if (c.flags & SPRINT_CONSUMER_DEAD)
          status = "DEAD";
        else if (c.flags & SPRINT_CONSUMER_SLOW_DROPPED)
          status = "SLOW_DROPPED";
        else if (c.flags & SPRINT_CONSUMER_ACTIVE)
          status = "ACTIVE";
        else
          status = "unknown";

        char row[80];
        snprintf(row, sizeof(row), "  %2u   %-12" PRIu64 "  %6" PRId64 "   %s",
                 i, c.read_seq, behind, status);
        std::cout << row << "\n";
      }
    }

    std::cout << std::flush;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  munmap(h, sizeof(ShmHeader));
}

#pragma endregion

#pragma region Consumer

void run_consumer() {
  DemoConsumer<PosixShmBackend> cons(shm_name);
  std::cout << "[demo_consumer] slot=" << cons.consumer_id() << "\n";

  uint64_t count = 0, skipped = 0, prev = 0;
  while (is_running) {
    auto f = cons.wait_next(200'000'000ULL);
    if (!f)
      continue;
    skipped += (prev > 0) ? f->seq - prev - 1 : 0;
    prev = f->seq;
    ++count;
    if (count % 20 == 0)
      std::cout << "[demo_consumer] seq=" << f->seq
                << " pts=" << f->num_elements << " skipped=" << skipped
                << (f->was_dropped ? " [RECOVERED]" : "") << "\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
};

#pragma endregion

#ifdef SPRINT_IPC_ENABLE_CUDA

#pragma region Zero-Copy Producer/Consumer

void run_producer_zero_copy() {
  if (!cuda_supports_zero_copy()) {
    std::cerr << "[zero_copy_producer] cudaHostRegisterMapped not supported on "
                 "this device\n";
    return;
  }

  std::default_random_engine gen;
  std::uniform_real_distribution<float> distribution(0.0, 1.0);

  constexpr uint32_t num_elements = 1;
  DemoProducer<CudaZeroCopyBackend> prod(shm_name, num_elements);

  std::cout << "[demo_producer_zero_copy:" << prod.ring_size()
            << "-slot ring] started\n";

  uint64_t frame = 0;
  while (is_running) {
    auto t0 = std::chrono::steady_clock::now();

    // Host pointer is directly accessible from CUDA kernels on Jetson (shared
    // VA).
    prod.next_slot_ptr()->number = distribution(gen);

    uint32_t dropped = prod.publish(num_elements);

    if (dropped)
      std::cerr << "[demo_producer_zero_copy] frame=" << frame << " dropped "
                << dropped << " consumer(s)\n";

    auto rem = std::chrono::microseconds(50000) -
               (std::chrono::steady_clock::now() - t0);
    if (rem > std::chrono::microseconds(0))
      std::this_thread::sleep_for(rem);
    ++frame;
  }
}

void run_consumer_zero_copy() {
  if (!cuda_supports_zero_copy()) {
    std::cerr << "[zero_copy_consumer] cudaHostRegisterMapped not supported on "
                 "this device\n";
    return;
  }

  DemoConsumer<CudaZeroCopyBackend> cons(shm_name);
  std::cout << "[demo_consumer_zero_copy] slot=" << cons.consumer_id() << "\n";

  uint64_t count = 0, skipped = 0, prev = 0;
  while (is_running) {
    // f->ptr is a host pointer registered with CUDA; on Jetson it's also the
    // device pointer — no cudaMemcpy needed.
    auto f = cons.wait_next(200'000'000ULL);
    if (!f)
      continue;
    skipped += (prev > 0) ? f->seq - prev - 1 : 0;
    prev = f->seq;
    ++count;
    if (count % 20 == 0)
      std::cout << "[demo_consumer_zero_copy] seq=" << f->seq
                << " number=" << f->ptr->number << " skipped=" << skipped
                << (f->was_dropped ? " [RECOVERED]" : "") << "\n";
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
}

#pragma endregion

#pragma region CUDA Producer/Consumer

void run_producer_cuda() {
  if (!cuda_supports_ipc()) {
    std::cerr
        << "[cuda_producer] CUDA IPC memory handles not supported on this "
           "device (on Jetson, use zero_copy_producer instead)\n";
    return;
  }

  std::default_random_engine gen;
  std::uniform_real_distribution<float> distribution(0.0, 1.0);

  constexpr uint32_t num_elements = 1;
  DemoProducer<CudaBackend> prod(shm_name, num_elements);

  std::cout << "[demo_producer_cuda:" << prod.ring_size()
            << "-slot ring] started\n";

  DemoPayload host_buf{};
  uint64_t frame = 0;
  while (is_running) {
    auto t0 = std::chrono::steady_clock::now();

    host_buf.number = distribution(gen);
    pcl_cuda_check(cudaMemcpy(prod.next_slot_ptr(), &host_buf,
                              sizeof(DemoPayload), cudaMemcpyHostToDevice),
                   "cudaMemcpy H->D");

    uint32_t dropped = prod.publish(num_elements);

    if (dropped)
      std::cerr << "[demo_producer_cuda] frame=" << frame << " dropped "
                << dropped << " consumer(s)\n";

    auto rem = std::chrono::microseconds(50000) -
               (std::chrono::steady_clock::now() - t0);
    if (rem > std::chrono::microseconds(0))
      std::this_thread::sleep_for(rem);
    ++frame;
  }
}

void run_consumer_cuda() {
  if (!cuda_supports_ipc()) {
    std::cerr
        << "[cuda_consumer] CUDA IPC memory handles not supported on this "
           "device (on Jetson, use zero_copy_consumer instead)\n";
    return;
  }

  DemoConsumer<CudaBackend> cons(shm_name);
  std::cout << "[demo_consumer_cuda] slot=" << cons.consumer_id() << "\n";

  DemoPayload host_buf{};
  uint64_t count = 0, skipped = 0, prev = 0;
  while (is_running) {
    // wait_next_and_consume copies device memory BEFORE advancing read_seq,
    // so the producer cannot reclaim the slot until the D->H copy is done.
    auto f = cons.wait_next_and_consume(
        [&](const IpcConsumer<DemoPayload, CudaBackend>::Frame &fr) {
          pcl_cuda_check(cudaMemcpy(&host_buf, fr.ptr,
                                    sizeof(DemoPayload) * fr.num_elements,
                                    cudaMemcpyDeviceToHost),
                         "cudaMemcpy D->H");
        },
        200'000'000ULL);

    if (!f)
      continue;

    skipped += (prev > 0) ? f->seq - prev - 1 : 0;
    prev = f->seq;
    ++count;
    if (count % 20 == 0)
      std::cout << "[demo_consumer_cuda] seq=" << f->seq
                << " num=" << f->num_elements << " number=" << host_buf.number
                << " skipped=" << skipped
                << (f->was_dropped ? " [RECOVERED]" : "") << "\n";

    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
}

#pragma endregion

#endif // SPRINT_IPC_ENABLE_CUDA

int main(int argc, char *argv[]) {
  signal(SIGINT, on_signal);
  signal(SIGTERM, on_signal);

  if (argc < 2) {
    return 1;
  }

  std::string role = argv[1];
  if (role == "producer") {
    run_producer();
  } else if (role == "consumer") {
    run_consumer();
  } else if (role == "stats") {
    run_stats();
#ifdef SPRINT_IPC_ENABLE_CUDA
  } else if (role == "zero_copy_producer") {
    run_producer_zero_copy();
  } else if (role == "zero_copy_consumer") {
    run_consumer_zero_copy();
  } else if (role == "cuda_producer") {
    run_producer_cuda();
  } else if (role == "cuda_consumer") {
    run_consumer_cuda();
#endif
  } else {
    return 2;
  }
};