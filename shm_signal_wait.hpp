#ifndef _SHM_SIGNAL_WAIT_HPP
#define _SHM_SIGNAL_WAIT_HPP

#include <poll.h>

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <system_error>

// Wait for the supplied signalfd to become readable. Returns true when a
// signal is pending, false on timeout, and throws for invalid descriptors or
// polling errors; it does not consume the signal.
inline bool wait_for_signal_or_timeout(int signal_fd, std::chrono::milliseconds timeout) {
  if (signal_fd < 0) {
    throw std::invalid_argument("signalfd must be a valid file descriptor");
  }

  pollfd descriptor{signal_fd, POLLIN, 0};
  for (;;) {
    const int ready = ::poll(&descriptor, 1, static_cast<int>(timeout.count()));
    if (ready == 0) {
      return false;
    }
    if (ready == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "poll signalfd");
    }
    if (descriptor.revents & POLLIN) {
      return true;
    }
    throw std::system_error(descriptor.revents & POLLNVAL ? EBADF : EIO, std::generic_category(),
                            "poll signalfd");
  }
}

#endif
