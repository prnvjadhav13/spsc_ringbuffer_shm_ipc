// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#ifndef _SPSC_RINGBUFFER_HPP
#define _SPSC_RINGBUFFER_HPP
#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

enum class EnqueueResult { enqueued, full };
enum class DequeueResult { dequeued, empty };

namespace spsc_ringbuffer_detail {

inline constexpr std::size_t kCacheLineSize = 64;

// Give every slot a cache-line-aligned start and a cache-line-multiple stride.
// This prevents adjacent records from sharing their boundary cache line.
template <typename T,
          std::size_t Padding =
              (kCacheLineSize - (sizeof(T) % kCacheLineSize)) % kCacheLineSize>
struct alignas(kCacheLineSize) CacheLineSlot {
  T value;
  std::array<std::byte, Padding> padding{};
};

template <typename T>
struct alignas(kCacheLineSize) CacheLineSlot<T, 0> {
  T value;
};

}  // namespace spsc_ringbuffer_detail

// Lock-free single-producer/single-consumer ring buffer for fixed-size,
// trivially copyable messages. The producer calls EnQueue and the consumer
// calls DeQueue; the result distinguishes a successful operation from full or
// empty queue states.
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free on this platform");
  static_assert(Capacity > 0, "Capacity must be non-zero");
  static_assert(std::is_trivially_copyable_v<T>,
                "T must be trivially copyable for shared-memory IPC");

 public:
  [[nodiscard]] EnqueueResult EnQueue(const T& item) {
    const auto tail = tail_.load(std::memory_order_relaxed);
    const auto head = head_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) >= Capacity) {
      return EnqueueResult::full;
    }

    buffer_[tail % Capacity].value = item;
    tail_.store(tail + 1, std::memory_order_release);
    return EnqueueResult::enqueued;
  }

  [[nodiscard]] EnqueueResult EnQueue(T&& item) {
    const auto tail = tail_.load(std::memory_order_relaxed);
    const auto head = head_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) >= Capacity) {
      return EnqueueResult::full;
    }

    buffer_[tail % Capacity].value = std::move(item);
    tail_.store(tail + 1, std::memory_order_release);
    return EnqueueResult::enqueued;
  }

  [[nodiscard]] DequeueResult DeQueue(T& item) {
    const auto head = head_.load(std::memory_order_relaxed);
    const auto tail = tail_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) == 0) {
      return DequeueResult::empty;
    }

    item = std::move(buffer_[head % Capacity].value);
    head_.store(head + 1, std::memory_order_release);
    return DequeueResult::dequeued;
  }

  [[nodiscard]] bool IsEmpty() const noexcept {
    return (Size() == 0);
  }

  [[nodiscard]] bool IsFull() const noexcept {
    return (Size() >= Capacity);
  }

  [[nodiscard]] std::size_t Size() const noexcept {
    const auto head = head_.load(std::memory_order_acquire);
    const auto tail = tail_.load(std::memory_order_acquire);
    assert(tail >= head);
    const auto size = tail - head;
    return size <= Capacity ? static_cast<std::size_t>(size) : Capacity;
  }

 private:
  static constexpr std::size_t cache_line_size = spsc_ringbuffer_detail::kCacheLineSize;
  alignas(cache_line_size) std::atomic<std::uint64_t> head_{0};
  alignas(cache_line_size) std::atomic<std::uint64_t> tail_{0};
  std::array<spsc_ringbuffer_detail::CacheLineSlot<T>, Capacity> buffer_{};
};

// Alternate SPSC implementation retained for comparison/examples. Prefer
// SpscRingBuffer for the shared-memory queue used by the IPC applications.
template <typename T, std::size_t Capacity>
class SPSCQueue {
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable for shared memory");
  static_assert(Capacity > 0, "Capacity must be non-zero");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free on this platform");

  static constexpr std::size_t kCacheLine = spsc_ringbuffer_detail::kCacheLineSize;

 public:
  // Producer side: enqueue one element at the rear.
  // Returns full if no slot is available.
  EnqueueResult EnQueue(const T& item) noexcept {
    const auto rear = rear_.load(std::memory_order_relaxed);
    const auto front = front_.load(std::memory_order_acquire);
    if (rear - front >= Capacity)
      return EnqueueResult::full;
    data_[rear % Capacity].value = item;
    rear_.store(rear + 1, std::memory_order_release);
    return EnqueueResult::enqueued;
  }

  // Consumer side: dequeue one element from the front.
  // Returns empty if no element is available.
  DequeueResult DeQueue(T& item) noexcept {
    const auto front = front_.load(std::memory_order_relaxed);
    const auto rear = rear_.load(std::memory_order_acquire);
    if (front == rear)
      return DequeueResult::empty;
    item = std::move(data_[front % Capacity].value);
    front_.store(front + 1, std::memory_order_release);
    return DequeueResult::dequeued;
  }

 private:
  alignas(kCacheLine) std::atomic<std::uint64_t> rear_{0};   // producer writes
  alignas(kCacheLine) std::atomic<std::uint64_t> front_{0};  // consumer writes
  std::array<spsc_ringbuffer_detail::CacheLineSlot<T>, Capacity> data_{};
};
#endif
