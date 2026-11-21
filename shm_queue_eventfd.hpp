// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#ifndef _SHM_QUEUE_EVENTFD_HPP
#define _SHM_QUEUE_EVENTFD_HPP

#include <expected>
#include <system_error>
#include <stop_token>

// Move-only owner of one POSIX/Linux file descriptor. Destruction closes the
// descriptor unless release() transfers it to the caller.
class UniqueFileDescriptor {
 public:
  explicit UniqueFileDescriptor(int fd = -1) noexcept;
  ~UniqueFileDescriptor();

  UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
  UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;

  UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept;
  UniqueFileDescriptor& operator=(UniqueFileDescriptor&& other) noexcept;

  int get() const noexcept;
  explicit operator bool() const noexcept;
  int release() noexcept;
  void reset(int fd = -1) noexcept;

 private:
  int fd_;
};

struct SharedQueueNotificationFds {
  UniqueFileDescriptor queue_ready;
  UniqueFileDescriptor space_available;
};

[[nodiscard]] std::expected<void, std::error_code> notify_eventfd(int fd) noexcept;
[[nodiscard]] std::expected<void, std::error_code> drain_eventfd(int fd) noexcept;

// Producer-side eventfd notification transport. It passes a producer-to-
// consumer queue-ready eventfd and a consumer-to-producer space-available
// eventfd to the consumer over a Unix-domain socket.
// The actual queue creation, mapping, enqueueing, and dequeueing belong to
// SharedLogQueueMapping. SharedQueueEventFd handles the wake-up mechanism and the
// initial descriptor handoff; queue records travel through shared memory.
class SharedQueueEventFd {
 public:
  SharedQueueEventFd();

  [[nodiscard]] std::expected<void, std::error_code> notify() const noexcept;
  [[nodiscard]] int space_available_fd() const noexcept;
  void serve_consumers(std::stop_token stop) const;
  static SharedQueueNotificationFds receive(int signal_fd);

 private:
  void send_eventfds(int connection) const;

  UniqueFileDescriptor queue_ready_fd_;
  UniqueFileDescriptor space_available_fd_;
  UniqueFileDescriptor listener_fd_;
};

#endif
