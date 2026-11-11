// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#ifndef _SHM_LOG_QUEUE_MAPPING_HPP
#define _SHM_LOG_QUEUE_MAPPING_HPP

#include <semaphore.h>

#include <chrono>
#include <cstddef>
#include <type_traits>

#include "log_queue_config.hpp"
#include "shared_queue_storage.hpp"

// Per-process RAII handle for a shared SPSC queue. It owns this process's
// mapping, file descriptor, semaphore handles, and (for the producer) the
// names it created; it does not exclusively own the queue shared with peers.
// Producer API: create_shared_memory_ring_buffer, enqueue,
// mark_producer_done, and readiness methods. Consumer API:
// open_shared_memory_ring_buffer, dequeue, producer_done, and readiness
// methods. Role checks reject operations used by the wrong endpoint.
template <typename T, std::size_t Capacity>
class SharedLogQueueMapping {
  static_assert(Capacity > 0, "Queue capacity must be non-zero");
  static_assert(std::is_trivially_copyable_v<T>,
                "Shared queue messages must be trivially copyable");

 public:
  SharedLogQueueMapping() = default;
  ~SharedLogQueueMapping() noexcept;

  SharedLogQueueMapping(const SharedLogQueueMapping&) = delete;
  SharedLogQueueMapping& operator=(const SharedLogQueueMapping&) = delete;

  SharedLogQueueMapping(SharedLogQueueMapping&& other) noexcept;
  SharedLogQueueMapping& operator=(SharedLogQueueMapping&& other) noexcept;

  void create_shared_memory_ring_buffer();
  bool open_shared_memory_ring_buffer(int signal_fd);
  [[nodiscard]] bool enqueue(const T& message);
  [[nodiscard]] bool dequeue(T& message);
  [[nodiscard]] std::size_t queue_size() const;
  void mark_producer_done();
  [[nodiscard]] bool producer_done() const;
  void post_ready() const;
  bool wait_until_ready(int signal_fd) const;

 private:
  enum class Role { none, producer, consumer };

  void require_role(Role expected, const char* operation) const;
  static void throw_errno(const char* operation);
  void take_resources(SharedLogQueueMapping& other) noexcept;
  void reset() noexcept;
  static void report_cleanup_error(const char* operation) noexcept;

  int fd_{-1};
  void* mapping_{nullptr};
  SharedQueueStorage<T, Capacity>* shared_{nullptr};
  sem_t* ready_{SEM_FAILED};
  bool owns_shared_memory_name_{false};
  bool owns_sem_ready_name_{false};
  Role role_{Role::none};
};

#endif
