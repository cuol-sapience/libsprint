#ifndef SPRINT_IPC_DEMO_PAYLOAD_HPP_
#define SPRINT_IPC_DEMO_PAYLOAD_HPP_

#include <cstdint>
#include <sprint/ipc/consumer.hpp>
#include <sprint/ipc/payload/common.hpp>
#include <sprint/ipc/producer.hpp>
#include <string>

struct DemoPayload {
  float number;
};

template <>
struct sprint::ipc::TypeTraits<DemoPayload> : DefaultTypeTraits<DemoPayload> {
  // TODO: implement child_magic, child_version

  static const std::string name() { return "DemoPayload"; }
  static constexpr uint32_t id = 0x57535320u; // "WSS "
  static constexpr uint32_t max_elements = 1;
  static constexpr uint32_t ring_size = 8;
  static constexpr OverwritePolicy policy = OverwritePolicy::STALL_PER_CONSUMER;
};

template <typename Backend = sprint::ipc::DefaultBackend>
using DemoProducer = sprint::ipc::IpcProducer<DemoPayload, Backend>;

template <typename Backend = sprint::ipc::DefaultBackend>
using DemoConsumer = sprint::ipc::IpcConsumer<DemoPayload, Backend>;

#endif /* SPRINT_IPC_DEMO_PAYLOAD_HPP_ */