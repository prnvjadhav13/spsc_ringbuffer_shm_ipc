// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#include <cstdint>
#include <cstring>
#include <iostream>
#include <string_view>

#include "spsc_ringbuffer.hpp"

// Check FIFO ordering across full-queue retries and draining for the requested item count.
template <std::size_t Capacity>
bool test_RingBuffer(int number) {
  if (number < 0) {
    std::cerr << "Number of items must not be negative.\n";
    return false;
  }

  SpscRingBuffer<int, Capacity> ringbuffer;
  std::int64_t expected = 1;

  // Dequeue one item and verify that it matches the next expected FIFO value.
  const auto dequeue_expected = [&]() {
    int value = 0;
    if (!ringbuffer.DeQueue(value)) {
      std::cerr << "Dequeue failed while expecting " << expected << ".\n";
      return false;
    }

    std::cout << "Dequeued: " << value << '\n';
    if (value != expected) {
      std::cerr << "Expected " << expected << ", got " << value << ".\n";
      return false;
    }

    ++expected;
    return true;
  };

  for (std::int64_t value = 1; value <= number; ++value) {
    const int item = static_cast<int>(value);
    if (!ringbuffer.EnQueue(item)) {
      if (!dequeue_expected()) {
        return false;
      }
      if (!ringbuffer.EnQueue(item)) {
        std::cerr << "Enqueue failed after making queue space.\n";
        return false;
      }
    }

    std::cout << "Enqueued: " << item << '\n';
  }

  while (expected <= number) {
    if (!dequeue_expected()) {
      return false;
    }
  }

  return true;
}

// Run the integer FIFO test and demonstrate bounded message storage in a capacity-one queue.
int main() {
  constexpr int count = 100;
  constexpr std::size_t queue_capacity = 16;
  const bool passed = test_RingBuffer<queue_capacity>(count);
  std::cout << "Test result: " << (passed ? "PASS" : "FAIL") << '\n';

  struct Message {
    std::uint32_t length{};
    char text[128]{};
  };
  SpscRingBuffer<Message, 1> ringbuffer;

  constexpr std::string_view messages[] = {"Hello", "World", "Universe", "Multi-Verse"};
  for (const auto& text : messages) {
    Message message{};
    if (text.size() > sizeof(message.text)) {
      std::cerr << "Message exceeds the text buffer capacity.\n";
      return 1;
    }
    message.length = static_cast<std::uint32_t>(text.size());
    std::memcpy(message.text, text.data(), message.length);
    if (!ringbuffer.EnQueue(std::move(message))) {
      std::cerr << "Message enqueue failed: queue is full.\n";
      std::cerr << "Message enqueue failed:" << message.text << std::endl;

      // return 1;
    }
  }

  Message output{};
  while (ringbuffer.DeQueue(output)) {
    std::cout << "Dequeued: ";
    std::cout.write(output.text, output.length);
    std::cout << '\n';
  }

  return passed ? 0 : 1;
}
