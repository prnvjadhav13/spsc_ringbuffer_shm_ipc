#ifndef _SHARED_QUEUE_STORAGE_HPP
#define _SHARED_QUEUE_STORAGE_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "spsc_ringbuffer.hpp"

inline constexpr std::uint64_t kSharedQueueMagic = 0x5348515545554531ULL;
inline constexpr std::uint32_t kSharedQueueVersion = 2;

// In-place shared-memory layout for one SPSC queue. It stores compatibility
// metadata, the ring buffer, and producer status flags; it owns no OS handles
// and performs no mapping or cleanup itself.
template <typename T, std::size_t Capacity>
struct SharedQueueStorage {
  static_assert(Capacity > 0, "Queue capacity must be non-zero");
  static_assert(std::is_trivially_copyable_v<T>,
                "Shared queue messages must be trivially copyable");

  std::uint64_t magic{kSharedQueueMagic};
  std::uint32_t version{kSharedQueueVersion};
  std::uint64_t capacity{Capacity};
  std::uint64_t message_size{sizeof(T)};
  std::uint64_t message_alignment{alignof(T)};
  SpscRingBuffer<T, Capacity> ring;
  std::atomic<bool> producer_done{false};
};

static_assert(std::atomic<bool>::is_always_lock_free,
              "Shared queue status requires lock-free boolean atomics");

#endif
