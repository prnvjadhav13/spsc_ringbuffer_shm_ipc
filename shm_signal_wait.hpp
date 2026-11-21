// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#ifndef _SHM_SIGNAL_WAIT_HPP
#define _SHM_SIGNAL_WAIT_HPP

#include <poll.h>

#include <cerrno>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <system_error>

enum class SignalWaitResult { signal_received, timed_out };

// Wait for the supplied signalfd to become readable. Distinguishes a pending
// signal from timeout, throws for invalid descriptors or polling errors, and
// does not consume the signal.
inline SignalWaitResult wait_for_signal_or_timeout(int signal_fd,
                                                   std::chrono::milliseconds timeout) {
  if (signal_fd < 0) {
    throw std::invalid_argument("signalfd must be a valid file descriptor");
  }

  if (timeout.count() < std::numeric_limits<int>::min() ||
      timeout.count() > std::numeric_limits<int>::max()) {
    throw std::invalid_argument("poll timeout does not fit in an int");
  }

  pollfd descriptor{signal_fd, POLLIN, 0};
  for (;;) {
    const int ready = ::poll(&descriptor, 1, static_cast<int>(timeout.count()));
    if (ready == 0) {
      return SignalWaitResult::timed_out;
    }
    if (ready == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "poll signalfd");
    }
    if (descriptor.revents & POLLIN) {
      return SignalWaitResult::signal_received;
    }
    throw std::system_error(descriptor.revents & POLLNVAL ? EBADF : EIO, std::generic_category(),
                            "poll signalfd");
  }
}

#endif
