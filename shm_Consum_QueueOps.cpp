#include <pthread.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <system_error>

#include "log_queue_config.hpp"
#include "shm_log_queue_mapping.hpp"
#include "shm_queue_eventfd.hpp"

#define EPOLL_EVENT_QUEUE 1
#define EPOLL_EVENT_SIGNAL 2

namespace {

// View a fixed-size text field up to its first null character or capacity.
std::string_view bounded_text(const char* text, std::size_t capacity) {
  const auto* end = std::find(text, text + capacity, '\0');
  return {text, static_cast<std::size_t>(end - text)};
}

// Convert a log severity to its printable name, with a fallback for unknown values.
const char* level_name(Level level) {
  switch (level) {
    case Level::DEBUG:
      return "DEBUG";
    case Level::INFO:
      return "INFO";
    case Level::WARN:
      return "WARN";
    case Level::ERROR:
      return "ERROR";
    case Level::CRITICAL:
      return "CRITICAL";
    case Level::FATAL:
      return "FATAL";
  }
  return "UNKNOWN";
}

// Format one log record into the supplied output stream using bounded text fields.
void write_event(std::ostream& output, const LogEvent& event) {
  output << event.LogEventId << ". IP: " << bounded_text(event.ip, sizeof(event.ip))
         << " | Level: " << level_name(event.level)
         << " | Message: " << bounded_text(event.message, sizeof(event.message)) << '\n';
}

// Read one pending signal from signalfd; return zero when none is available.
int read_signal(int signal_fd) {
  signalfd_siginfo signal_info{};
  for (;;) {
    const auto bytes_read = ::read(signal_fd, &signal_info, sizeof(signal_info));
    if (bytes_read == static_cast<ssize_t>(sizeof(signal_info))) {
      return static_cast<int>(signal_info.ssi_signo);
    }
    if (bytes_read == -1 && errno == EINTR) {
      continue;
    }
    if (bytes_read == -1 && errno == EAGAIN) {
      return 0;
    }
    if (bytes_read == -1) {
      throw std::system_error(errno, std::generic_category(), "read signalfd");
    }
    throw std::runtime_error("short read from signalfd");
  }
}

// Name the supported shutdown signals: SIGINT and SIGTERM.
const char* signal_name(int signal_number) {
  return signal_number == SIGINT ? "SIGINT" : "SIGTERM";
}

// Report the signal that interrupted startup and return its conventional exit status.
int exit_after_startup_signal(int signal_fd) {
  const int signal_number = read_signal(signal_fd);
  if (signal_number == 0) {
    throw std::runtime_error("Startup wait ended without a signal");
  }
  std::cout << "Received " << signal_name(signal_number) << "; consumer shutting down.\n";
  return 128 + signal_number;
}

}  // namespace

// Attach to the shared queue, drain notified records to console and file, and handle shutdown.
int main(int argc, char* argv[]) {
  if (argc > 2) {
    std::cerr << "Usage: " << argv[0] << " [output-file]\n";
    return 2;
  }

  sigset_t stop_signals;
  sigemptyset(&stop_signals);
  sigaddset(&stop_signals, SIGINT);
  sigaddset(&stop_signals, SIGTERM);
  // Block these signals so the consumer can read them from signalfd.
  const int mask_result = pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
  if (mask_result != 0) {
    std::cerr << "pthread_sigmask failed: " << std::strerror(mask_result) << '\n';
    return 1;
  }

  UniqueFileDescriptor signal_fd(::signalfd(-1, &stop_signals, SFD_CLOEXEC | SFD_NONBLOCK));
  if (!signal_fd) {
    std::cerr << "signalfd creation failed: " << std::strerror(errno) << '\n';
    return 1;
  }

  const char* output_path = argc == 2 ? argv[1] : kConsumerFilePath;
  std::ofstream log_file(output_path, std::ios::out | std::ios::app);
  if (!log_file) {
    std::cerr << "Could not open log output file: " << output_path << '\n';
    return 1;
  }

  try {
    SharedLogQueueMapping<LogEvent, kLogQueueCapacity> mapping;
    std::cout << "Waiting for producer to create shared memory...\n" << std::flush;
    if (!mapping.open_shared_memory_ring_buffer(signal_fd.get())) {
      return exit_after_startup_signal(signal_fd.get());
    }
    std::cout << "Shared memory mapped; waiting for producer initialization...\n" << std::flush;
    if (!mapping.wait_until_ready(signal_fd.get())) {
      return exit_after_startup_signal(signal_fd.get());
    }

    auto notification_fds = SharedQueueEventFd::receive(signal_fd.get());
    if (!notification_fds.queue_ready || !notification_fds.space_available) {
      return exit_after_startup_signal(signal_fd.get());
    }

    UniqueFileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
    if (!epoll_fd) {
      throw std::system_error(errno, std::generic_category(), "epoll_create1");
    }
    epoll_event queue_event{};
    queue_event.events = EPOLLIN;
    // data.u32 identifies which registered descriptor epoll_wait reports.
    queue_event.data.u32 = EPOLL_EVENT_QUEUE;
    if (::epoll_ctl(epoll_fd.get(), EPOLL_CTL_ADD, notification_fds.queue_ready.get(),
                    &queue_event) == -1) {
      throw std::system_error(errno, std::generic_category(), "epoll_ctl add queue eventfd");
    }
    epoll_event signal_event{};
    signal_event.events = EPOLLIN;
    signal_event.data.u32 = EPOLL_EVENT_SIGNAL;
    if (::epoll_ctl(epoll_fd.get(), EPOLL_CTL_ADD, signal_fd.get(), &signal_event) == -1) {
      throw std::system_error(errno, std::generic_category(), "epoll_ctl add signalfd");
    }

    std::size_t events_written = 0;
    bool output_ok = true;
    int received_signal = 0;
    bool queue_notified = false;
    bool signal_notified = false;

    while (true) {
      std::array<epoll_event, 2> ready_events{};
      std::cout << "Consumer is pausing at lower Queue watermark: " << mapping.queue_size() << '\n';
      const int ready_count = ::epoll_wait(epoll_fd.get(), ready_events.data(),
                                           static_cast<int>(ready_events.size()), -1);
      if (ready_count == -1) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(), "epoll_wait");
      }

      for (int i = 0; i < ready_count; ++i) {
        queue_notified |= ready_events[i].data.u32 == EPOLL_EVENT_QUEUE;
        signal_notified |= ready_events[i].data.u32 == EPOLL_EVENT_SIGNAL;
      }
      if (signal_notified) {
        received_signal = read_signal(signal_fd.get());
        if (received_signal != 0) {
          std::cout << "Received " << signal_name(received_signal)
                    << "; draining queued events before shutdown.\n";
          // Treat the signal as a queue wake-up to drain records already enqueued.
          queue_notified = true;
        }
      }

      if (queue_notified) {
        std::cout << "Consumer is waking up due to data in Queue: " << mapping.queue_size()
                  << std::endl;
        const int drain_error = drain_eventfd(notification_fds.queue_ready.get());
        queue_notified = false;
        if (drain_error != 0) {
          std::cerr << "Queue notification drain failed: " << std::strerror(-drain_error) << ".\n";
          return 1;
        }
        LogEvent event{};
        while (mapping.dequeue(event)) {
          const std::size_t queue_size = mapping.queue_size();

          if ((queue_size != 0) && queue_size <= kLogQueueLowWatermark) {
            const int notify_error = notify_eventfd(notification_fds.space_available.get());
            if (notify_error != 0) {
              std::cerr << "Space-available notification failed: " << std::strerror(-notify_error)
                        << ".\n";
              output_ok = false;
            }
          }

          if (output_ok) {
            write_event(std::cout, event);
            write_event(log_file, event);
            if (!std::cout || !log_file) {
              std::cerr << "Failed while writing a log event to the console or file.\n";
              output_ok = false;
            } else {
              ++events_written;
            }
          }
        }

        if (mapping.producer_done() && mapping.queue_size() == 0) {
          break;
        }
      }
      if (received_signal != 0) {
        break;
      }
    }

    log_file.flush();
    if (!log_file) {
      std::cerr << "Failed to flush log output file: " << output_path << '\n';
      output_ok = false;
    }
    if (output_ok) {
      std::cout << "Wrote " << events_written << " events to " << output_path << ".\n";
    }

    if (received_signal != 0) {
      std::cout << "Consumer stopped by " << signal_name(received_signal) << ".\n";
    }
    return output_ok ? (received_signal == 0 ? 0 : 128 + received_signal) : 1;
  } catch (const std::exception& error) {
    std::cerr << "Consumer setup or runtime failed: " << error.what() << '\n';
    return 1;
  }
}
