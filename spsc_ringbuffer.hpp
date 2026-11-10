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

// Lock-free single-producer/single-consumer ring buffer for fixed-size,
// trivially copyable messages. The producer calls EnQueue and the consumer
// calls DeQueue; false indicates full or empty, respectively.
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free on this platform");
  static_assert(Capacity > 0, "Capacity must be non-zero");
  static_assert(std::is_trivially_copyable_v<T>,
                "T must be trivially copyable for shared-memory IPC");

 public:
  [[nodiscard]] bool EnQueue(const T& item) {
    const auto tail = tail_.load(std::memory_order_relaxed);
    const auto head = head_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) >= Capacity) {
      return false;
    }

    buffer_[tail % Capacity] = item;
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool EnQueue(T&& item) {
    const auto tail = tail_.load(std::memory_order_relaxed);
    const auto head = head_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) >= Capacity) {
      return false;
    }

    buffer_[tail % Capacity] = std::move(item);
    tail_.store(tail + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool DeQueue(T& item) {
    const auto head = head_.load(std::memory_order_relaxed);
    const auto tail = tail_.load(std::memory_order_acquire);

    assert(tail >= head);

    if ((tail - head) == 0) {
      return false;
    }

    item = std::move(buffer_[head % Capacity]);
    head_.store(head + 1, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool IsEmpty() const noexcept {
    return (Size() == 0);
  }

  [[nodiscard]] bool IsFull() const noexcept {
    return (Size() >= Capacity);
  }

  [[nodiscard]] std::size_t Size() const noexcept {
    const auto tail = tail_.load(std::memory_order_acquire);
    const auto head = head_.load(std::memory_order_acquire);
    assert(tail >= head);
    const auto size = tail - head;
    return size <= Capacity ? static_cast<std::size_t>(size) : Capacity;
  }

 private:
  static constexpr std::size_t cache_line_size = 64;
  alignas(cache_line_size) std::atomic<std::uint64_t> head_{0};
  alignas(cache_line_size) std::atomic<std::uint64_t> tail_{0};
  alignas(cache_line_size) std::array<T, Capacity> buffer_{};
};

// Alternate SPSC implementation retained for comparison/examples. Prefer
// SpscRingBuffer for the shared-memory queue used by the IPC applications.
template <typename T, std::size_t Capacity>
class SPSCQueue {
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable for shared memory");
  static_assert(Capacity > 0, "Capacity must be non-zero");
  static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
                "64-bit atomics must be lock-free on this platform");

  static constexpr std::size_t kCacheLine = 64;

 public:
  // Producer side: enqueue one element at the rear.
  // Returns false if the queue is full.
  bool EnQueue(const T& item) noexcept {
    const auto rear = rear_.load(std::memory_order_relaxed);
    const auto front = front_.load(std::memory_order_acquire);
    if (rear - front >= Capacity)
      return false;  // full
    data_[rear % Capacity] = item;
    rear_.store(rear + 1, std::memory_order_release);
    return true;
  }

  // Consumer side: dequeue one element from the front.
  // Returns false if the queue is empty.
  bool DeQueue(T& item) noexcept {
    const auto front = front_.load(std::memory_order_relaxed);
    const auto rear = rear_.load(std::memory_order_acquire);
    if (front == rear)
      return false;  // empty
    item = data_[front % Capacity];
    front_.store(front + 1, std::memory_order_release);
    return true;
  }

 private:
  alignas(kCacheLine) std::atomic<std::uint64_t> rear_{0};   // producer writes
  alignas(kCacheLine) std::atomic<std::uint64_t> front_{0};  // consumer writes
  alignas(kCacheLine) T data_[Capacity];
};
#endif