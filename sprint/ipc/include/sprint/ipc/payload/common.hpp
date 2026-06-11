#ifndef SPRINT_IPC_PAYLOAD_COMMON_HPP_
#define SPRINT_IPC_PAYLOAD_COMMON_HPP_

#include <cstdint>
#include <stdexcept>
namespace sprint::ipc {

template <typename T> struct DefaultTypeTraits {
  static void validate(uint32_t n, uint32_t max) {
    if (n == 0 || n > max)
      throw std::runtime_error(+"sprint: publish() n=" + std::to_string(n) +
                               " out of range [1, " + std::to_string(max) +
                               "]");
  }
};

// don't put validate in base class so missing it causes compile error
template <typename T> struct TypeTraits {
  /*
  uint32_t child_magic; // parent is the shm header itself
  uint32_t child_version;
  std::string name; // type name
  uint32_t id;      // type id
  uint32_t default_max_elements;
*/
};

}; // namespace sprint::ipc

#endif /* SPRINT_IPC_PAYLOAD_COMMON_HPP_ */