// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <functional>
#include <iostream>
#include <system_error>
#include <thread>

#include "log_queue_config.hpp"
#include "shm_log_queue_mapping.hpp"
#include "shm_queue_eventfd.hpp"

namespace {

constexpr std::array<const char*, 6> kLevelNames{"DEBUG", "INFO",     "WARN",
                                                 "ERROR", "CRITICAL", "FATAL"};

constexpr std::array<const char*, 7> kMessages{
    "login success",     "HTTP 200 OK",         "login failure - Incorrect password",
    "dns failure",       "certificate expired", "TLS handshake failure",
    "connection refused"};

constexpr std::uint32_t kSpaceAvailableEvent = 1;
constexpr std::uint32_t kSignalEvent = 2;
constexpr std::uint32_t kTimeoutEvent = 3;
constexpr int kRunDurationSeconds = 20;

// Register a readable descriptor and its event identifier with epoll.
void watch_descriptor(int epoll_fd, int fd, std::uint32_t id) {
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u32 = id;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl");
  }
}

// Generate and enqueue log events, pausing at the high watermark until the queue drains.
int produce_events(SharedLogQueueMapping<LogEvent, kLogQueueCapacity>& mapping,
                   const SharedQueueEventFd& event_notifications, int signal_fd, int timer_fd,
                   const std::atomic<int>& server_error) {
  std::size_t event_count = 0;
  bool paused_for_watermark = false;
  int producer_error = 0;

  UniqueFileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
  if (!epoll_fd) {
    throw std::system_error(errno, std::generic_category(), "epoll_create1");
  }
  watch_descriptor(epoll_fd.get(), event_notifications.space_available_fd(), kSpaceAvailableEvent);
  watch_descriptor(epoll_fd.get(), signal_fd, kSignalEvent);
  watch_descriptor(epoll_fd.get(), timer_fd, kTimeoutEvent);

  for (;;) {
    if ((producer_error = server_error.load()) != 0) {
      break;
    }
    if (paused_for_watermark && mapping.queue_size() <= kLogQueueLowWatermark) {
      paused_for_watermark = false;
      std::cout << "Producer waking up due to low Queue watermark " << mapping.queue_size() << '\n';
    }

    // Poll shutdown while producing; block only under backpressure. Signals and
    // the deadline wake this same wait even if no consumer ever connects.
    std::array<epoll_event, 3> ready_events{};
    const int ready_count = ::epoll_wait(epoll_fd.get(), ready_events.data(), ready_events.size(),
                                         paused_for_watermark ? -1 : 0);
    if (ready_count == -1) {
      if (errno == EINTR) {
        continue;
      }
      producer_error = -errno;
      break;
    }

    bool shutdown_requested = false;
    for (int i = 0; i < ready_count; ++i) {
      const auto& event = ready_events[i];
      if ((event.events & (EPOLLERR | EPOLLHUP)) || !(event.events & EPOLLIN)) {
        producer_error = -EIO;
        break;
      }
      if (event.data.u32 == kSignalEvent) {
        signalfd_siginfo info{};
        ssize_t bytes;
        do {
          bytes = ::read(signal_fd, &info, sizeof(info));
        } while (bytes == -1 && errno == EINTR);
        if (bytes != static_cast<ssize_t>(sizeof(info))) {
          producer_error = bytes == -1 ? -errno : -EIO;
          break;
        }
        std::cout << "Received " << (info.ssi_signo == SIGINT ? "SIGINT" : "SIGTERM")
                  << "; stopping producer.\n";
        shutdown_requested = true;
      } else if (event.data.u32 == kTimeoutEvent) {
        // The timer is one-shot and is closed after this loop returns.
        std::cout << "20-second timeout elapsed; stopping producer.\n";
        shutdown_requested = true;
      } else if (event.data.u32 == kSpaceAvailableEvent) {
        producer_error = drain_eventfd(event_notifications.space_available_fd());
        if (producer_error != 0) {
          break;
        }
      } else {
        producer_error = -EIO;
        break;
      }
    }
    if (shutdown_requested || producer_error != 0) {
      break;
    }
    if (paused_for_watermark) {
      // Recheck actual occupancy on the next iteration, not just the notification.
      continue;
    }

    LogEvent event{};
    event.LogEventId = event_count + 1;
    std::snprintf(event.ip, sizeof(event.ip), "10.10.10.%zu", (event_count % 254) + 1);
    event.level = static_cast<Level>(event_count % kLevelNames.size());
    std::snprintf(event.message, sizeof(event.message), "%s",
                  kMessages[event_count % kMessages.size()]);

    if (!mapping.enqueue(event)) {
      // The generated event remains reproducible from event_count and is
      // retried after backpressure clears; the producer never skips its ID.
      paused_for_watermark = true;
      continue;
    }

    // Always count and notify consumer on a successful enqueue before pausing.
    ++event_count;
    const int notify_error = event_notifications.notify();
    if (notify_error != 0) {
      producer_error = notify_error;
      break;
    }
    const auto queue_size = mapping.queue_size();
    if (queue_size >= kLogQueueHighWatermark) {
      paused_for_watermark = true;
      std::cout << "Producer pausing at high Queue watermark " << queue_size << '\n';
    }
  }

  mapping.mark_producer_done();
  const int notify_error = event_notifications.notify();
  if (producer_error == 0 && notify_error != 0) {
    producer_error = notify_error;
  }

  std::cout << "Producer stopped after enqueueing " << event_count << " events.\n";
  if (producer_error != 0) {
    std::cerr << "Producer failed: " << std::strerror(-producer_error) << " (" << producer_error
              << ").\n";
  }
  return producer_error != 0 ? 1 : 0;
}

// Run the descriptor handoff server and report exceptions within its worker thread.
void notification_server_thread_entry(std::stop_token stop,
                                      const SharedQueueEventFd& event_notifications,
                                      std::atomic<int>& server_error) {
  try {
    event_notifications.serve_consumers(stop);
  } catch (const std::exception& error) {
    std::cerr << "Notification server failed: " << error.what() << '\n';
    server_error = -EIO;
    // Wake the producer if it is blocked waiting for queue space.
    (void)notify_eventfd(event_notifications.space_available_fd());
  }
}

}  // namespace

// Block shutdown signals before starting workers and create their signalfd.
int setup_signals_init() {
  sigset_t stop_signals;
  sigemptyset(&stop_signals);
  sigaddset(&stop_signals, SIGINT);
  sigaddset(&stop_signals, SIGTERM);
  const int mask_result = pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
  if (mask_result != 0) {
    throw std::system_error(mask_result, std::generic_category(), "pthread_sigmask");
  }

  return ::signalfd(-1, &stop_signals, SFD_CLOEXEC | SFD_NONBLOCK);
}

// Arm an owned one-shot timer; RAII closes it if configuration fails.
int setup_timers_init() {
  UniqueFileDescriptor timer_fd(::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK));
  if (!timer_fd) {
    throw std::system_error(errno, std::generic_category(), "timerfd_create");
  }
  itimerspec deadline{};
  deadline.it_value.tv_sec = kRunDurationSeconds;
  if (::timerfd_settime(timer_fd.get(), 0, &deadline, nullptr) == -1) {
    throw std::system_error(errno, std::generic_category(), "timerfd_settime");
  }
  return timer_fd.release();
}

// Initialize IPC and shutdown descriptors, run producer, then join the handoff server.
int main() {
  try {
    int sig_raw_fd = setup_signals_init();
    UniqueFileDescriptor signal_fd(sig_raw_fd);
    if (!signal_fd) {
      throw std::system_error(errno, std::generic_category(), "signalfd");
    }

    int timer_raw_fd = setup_timers_init();
    UniqueFileDescriptor timer_fd(timer_raw_fd);
    if (!timer_fd) {
      throw std::system_error(errno, std::generic_category(), "timerfd_create");
    }

    SharedQueueEventFd event_notifications;
    SharedLogQueueMapping<LogEvent, kLogQueueCapacity> mapping;
    mapping.create_shared_memory_ring_buffer();
    std::atomic<int> server_error{0};
    std::jthread notification_server(notification_server_thread_entry,
                                     std::cref(event_notifications), std::ref(server_error));
    mapping.post_ready();

    std::cout << "Initialized shared-memory log queue.\n";
    std::cout << "Producing log events continuously. Press Ctrl+C to stop.\n";

    // Production and shutdown share one event loop; no blocked producer thread to join.
    const int exit_status =
        produce_events(mapping, event_notifications, signal_fd.get(), timer_fd.get(), server_error);
    notification_server.request_stop();
    notification_server.join();
    return server_error.load() != 0 ? 1 : exit_status;
  } catch (const std::exception& error) {
    std::cerr << "Producer setup or runtime failed: " << error.what() << '\n';
    return 1;
  }
}
