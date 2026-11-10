#include "shm_queue_eventfd.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include "shm_signal_wait.hpp"

namespace {

constexpr std::string_view kSocketName = "shm_log_ring_buffer_eventfd";

// Build the abstract Unix-domain socket address used for descriptor handoff.
sockaddr_un socket_address() {
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  address.sun_path[0] = '\0';
  std::memcpy(address.sun_path + 1, kSocketName.data(), kSocketName.size());
  return address;
}

// Return the address length, including the abstract namespace marker.
socklen_t address_length() {
  return static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + 1 + kSocketName.size());
}

// Create an owned, nonblocking eventfd that is closed on exec.
UniqueFileDescriptor create_eventfd() {
  const int fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  if (fd == -1) {
    throw std::system_error(errno, std::generic_category(), "eventfd");
  }
  return UniqueFileDescriptor(fd);
}

// Bind and listen on the producer socket that accepts notification clients.
UniqueFileDescriptor create_listener() {
  UniqueFileDescriptor listener(
      ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
  if (!listener) {
    throw std::system_error(errno, std::generic_category(), "create notification listener");
  }
  const auto address = socket_address();
  if (::bind(listener.get(), reinterpret_cast<const sockaddr*>(&address), address_length()) == -1) {
    throw std::system_error(errno, std::generic_category(), "bind notification listener");
  }
  if (::listen(listener.get(), 4) == -1) {
    throw std::system_error(errno, std::generic_category(), "listen for notification clients");
  }
  return listener;
}

// Receive and validate both eventfds via SCM_RIGHTS; close descriptors on malformed handoff.
SharedQueueNotificationFds receive_eventfds(int connection) {
  std::array<char, 2> payload{};
  iovec data{payload.data(), payload.size()};
  alignas(cmsghdr) std::array<char, CMSG_SPACE(2 * sizeof(int))> control{};
  msghdr message{};
  message.msg_iov = &data;
  message.msg_iovlen = 1;
  message.msg_control = control.data();
  message.msg_controllen = control.size();

  ssize_t received;
  do {
    received = ::recvmsg(connection, &message, MSG_CMSG_CLOEXEC);
  } while (received == -1 && errno == EINTR);
  if (received == -1) {
    throw std::system_error(errno, std::generic_category(), "receive eventfd from producer");
  }

  std::array<int, 2> received_fds{-1, -1};
  std::size_t received_count = 0;
  bool invalid_control_message = false;
  for (cmsghdr* header = CMSG_FIRSTHDR(&message); header != nullptr;
       header = CMSG_NXTHDR(&message, header)) {
    if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS) {
      invalid_control_message = true;
      continue;
    }
    if (header->cmsg_len < CMSG_LEN(sizeof(int))) {
      invalid_control_message = true;
      continue;
    }

    const std::size_t descriptor_bytes = header->cmsg_len - CMSG_LEN(0);
    const std::size_t descriptor_count = descriptor_bytes / sizeof(int);
    invalid_control_message |= descriptor_bytes % sizeof(int) != 0 ||
                               descriptor_count != received_fds.size() || received_count != 0;
    const unsigned char* descriptors = CMSG_DATA(header);
    for (std::size_t i = 0; i < descriptor_count; ++i) {
      int received_fd = -1;
      std::memcpy(&received_fd, descriptors + (i * sizeof(received_fd)), sizeof(received_fd));
      if (received_count < received_fds.size()) {
        received_fds[received_count++] = received_fd;
      } else {
        ::close(received_fd);
      }
    }
  }
  if (received != static_cast<ssize_t>(payload.size()) || payload[0] != 'Q' || payload[1] != 'S' ||
      (message.msg_flags & (MSG_CTRUNC | MSG_TRUNC)) || invalid_control_message ||
      received_count != received_fds.size()) {
    for (std::size_t i = 0; i < received_count; ++i) {
      ::close(received_fds[i]);
    }
    throw std::runtime_error("invalid eventfd handoff from producer");
  }
  return {UniqueFileDescriptor(received_fds[0]), UniqueFileDescriptor(received_fds[1])};
}

}  // namespace

// Take ownership of a descriptor; -1 represents an empty owner.
UniqueFileDescriptor::UniqueFileDescriptor(int fd) noexcept : fd_(fd) {
}

// Close the owned descriptor when this wrapper leaves scope.
UniqueFileDescriptor::~UniqueFileDescriptor() {
  reset();
}

// Transfer descriptor ownership from another wrapper, leaving it empty.
UniqueFileDescriptor::UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {
}

// Close the current descriptor and take ownership from the moved wrapper.
UniqueFileDescriptor& UniqueFileDescriptor::operator=(UniqueFileDescriptor&& other) noexcept {
  if (this != &other) {
    reset();
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

// Expose the descriptor for system calls without transferring ownership.
int UniqueFileDescriptor::get() const noexcept {
  return fd_;
}

// Report whether this wrapper owns a descriptor.
UniqueFileDescriptor::operator bool() const noexcept {
  return fd_ != -1;
}

// Transfer descriptor ownership to the caller without closing it.
int UniqueFileDescriptor::release() noexcept {
  return std::exchange(fd_, -1);
}

// Close the current descriptor and adopt the replacement, if supplied.
void UniqueFileDescriptor::reset(int fd) noexcept {
  if (fd_ != -1) {
    ::close(fd_);
  }
  fd_ = fd;
}

// Increment the eventfd counter; return zero on success or saturation, otherwise negative errno.
int notify_eventfd(int fd) noexcept {
  constexpr std::uint64_t increment = 1;
  for (;;) {
    const auto written = ::write(fd, &increment, sizeof(increment));
    if (written == static_cast<ssize_t>(sizeof(increment))) {
      return 0;
    }
    if (written == -1 && errno == EINTR) {
      continue;
    }
    if (written == -1 && errno == EAGAIN) {
      return 0;
    }
    if (written == -1) {
      return -errno;
    }
    return -EIO;
  }
}

// Read a nonblocking eventfd until empty; return zero or a negative errno on failure.
int drain_eventfd(int fd) noexcept {
  for (;;) {
    std::uint64_t value = 0;
    const auto bytes_read = ::read(fd, &value, sizeof(value));
    if (bytes_read == static_cast<ssize_t>(sizeof(value))) {
      continue;
    }
    if (bytes_read == -1 && errno == EINTR) {
      continue;
    }
    if (bytes_read == -1 && errno == EAGAIN) {
      return 0;
    }
    if (bytes_read == -1) {
      return -errno;
    }
    return -EIO;
  }
}

// Create both notification eventfds and the producer-side handoff listener.
SharedQueueEventFd::SharedQueueEventFd()
    : queue_ready_fd_(create_eventfd()),
      space_available_fd_(create_eventfd()),
      listener_fd_(create_listener()) {
}

// Signal that queue data or producer completion is available for the consumer.
int SharedQueueEventFd::notify() const noexcept {
  return notify_eventfd(queue_ready_fd_.get());
}

// Expose the eventfd the producer watches for consumer space notifications.
int SharedQueueEventFd::space_available_fd() const noexcept {
  return space_available_fd_.get();
}

// Accept clients and pass both eventfds, polling periodically to observe stop requests.
void SharedQueueEventFd::serve_consumers(std::stop_token stop) const {
  while (!stop.stop_requested()) {
    pollfd descriptor{listener_fd_.get(), POLLIN, 0};
    const int ready = ::poll(&descriptor, 1, 100);
    if (ready == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "poll notification socket");
    }
    if (ready == 0) {
      continue;
    }
    if (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      throw std::runtime_error("notification socket poll failed");
    }
    if (!(descriptor.revents & POLLIN)) {
      continue;
    }

    UniqueFileDescriptor connection(::accept4(listener_fd_.get(), nullptr, nullptr, SOCK_CLOEXEC));
    if (!connection) {
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      throw std::system_error(errno, std::generic_category(), "accept notification socket");
    }
    send_eventfds(connection.get());
  }
}

// Connect to the producer and receive its eventfds; return empty owners if startup is interrupted.
SharedQueueNotificationFds SharedQueueEventFd::receive(int signal_fd) {
  for (;;) {
    UniqueFileDescriptor connection(::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0));
    if (!connection) {
      throw std::system_error(errno, std::generic_category(), "create notification socket");
    }

    const auto address = socket_address();
    if (::connect(connection.get(), reinterpret_cast<const sockaddr*>(&address),
                  address_length()) == -1) {
      const int connect_error = errno;
      if (connect_error == ENOENT || connect_error == ECONNREFUSED || connect_error == EINTR) {
        if (wait_for_signal_or_timeout(signal_fd, std::chrono::milliseconds(50))) {
          return {UniqueFileDescriptor{}, UniqueFileDescriptor{}};
        }
        continue;
      }
      throw std::system_error(connect_error, std::generic_category(),
                              "connect notification socket");
    }

    return receive_eventfds(connection.get());
  }
}

// Pass references to both kernel eventfds over a connected Unix socket using SCM_RIGHTS.
void SharedQueueEventFd::send_eventfds(int connection) const {
  std::array<char, 2> payload{'Q', 'S'};
  iovec data{payload.data(), payload.size()};
  alignas(cmsghdr) std::array<char, CMSG_SPACE(2 * sizeof(int))> control{};
  msghdr message{};
  message.msg_iov = &data;
  message.msg_iovlen = 1;
  message.msg_control = control.data();
  message.msg_controllen = control.size();

  cmsghdr* header = CMSG_FIRSTHDR(&message);
  header->cmsg_level = SOL_SOCKET;
  header->cmsg_type = SCM_RIGHTS;
  header->cmsg_len = CMSG_LEN(2 * sizeof(int));
  const std::array<int, 2> event_fds{queue_ready_fd_.get(), space_available_fd_.get()};
  std::memcpy(CMSG_DATA(header), event_fds.data(), sizeof(event_fds));

  for (;;) {
    const auto sent = ::sendmsg(connection, &message, MSG_NOSIGNAL);
    if (sent == static_cast<ssize_t>(payload.size())) {
      return;
    }
    if (sent == -1 && errno == EINTR) {
      continue;
    }
    if (sent == -1 &&
        (errno == EPIPE || errno == ECONNRESET || errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    if (sent == -1) {
      throw std::system_error(errno, std::generic_category(), "send eventfds to consumer");
    }
    throw std::runtime_error("short send while passing eventfds");
  }
}
