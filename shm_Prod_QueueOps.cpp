// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstring>
#include <expected>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <string_view>
#include <system_error>
#include <thread>

#include "log_queue_config.hpp"
#include "shm_log_queue_mapping.hpp"
#include "shm_queue_eventfd.hpp"
#include "transfer_stats.hpp"

namespace {

constexpr std::uint32_t kSpaceAvailableEvent = 1;
constexpr std::uint32_t kSignalEvent = 2;
constexpr auto kConsumerAttachGracePeriod = std::chrono::seconds(3);
constexpr auto kConsumerDrainGracePeriod = std::chrono::seconds(5);

enum class ParseSizeArgumentError { missing_value, invalid_integer };

std::expected<std::optional<int>, std::error_code> consume_signal(int signal_fd) {
  signalfd_siginfo info{};
  ssize_t bytes;
  do {
    bytes = ::read(signal_fd, &info, sizeof(info));
  } while (bytes == -1 && errno == EINTR);
  if (bytes != static_cast<ssize_t>(sizeof(info))) {
    if (bytes == -1 && errno == EAGAIN) {
      return std::nullopt;
    }
    return std::unexpected(
        std::error_code(bytes == -1 ? errno : EIO, std::generic_category()));
  }
  return std::optional<int>(static_cast<int>(info.ssi_signo));
}

// Register a readable descriptor and its event identifier with epoll.
void watch_descriptor(int epoll_fd, int fd, std::uint32_t id) {
  epoll_event event{};
  event.events = EPOLLIN;
  event.data.u32 = id;
  if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1) {
    throw std::system_error(errno, std::generic_category(), "epoll_ctl");
  }
}

void fill_random_payload(LogEvent& event, std::size_t payload_bytes,
                         std::mt19937_64& random_engine) {
  std::uniform_int_distribution<unsigned int> random_byte(0, 0xFF);
  std::generate_n(event.payload, payload_bytes, [&random_engine, &random_byte] {
    return static_cast<std::byte>(random_byte(random_engine));
  });
  event.message_length = payload_bytes;
}

int produce_messages(SharedLogQueueMapping<LogEvent, kLogQueueCapacity>& mapping,
                     const SharedQueueEventFd& event_notifications, int signal_fd,
                     const std::atomic<int>& server_error, const TransferOptions& options) {
  std::size_t event_count = 0;
  bool paused_for_watermark = false;
  int producer_error = 0;
  int received_signal = 0;
  bool consumer_attached = false;
  std::chrono::steady_clock::time_point consumer_attach_deadline{};
  std::chrono::steady_clock::time_point consumer_drain_deadline{};
  bool consumer_drain_deadline_set = false;
  bool shutdown_incomplete = false;
  std::uint64_t payload_total_bytes = 0;
  std::mt19937_64 random_engine;
  std::random_device random_device;
  random_engine.seed(random_device());
  std::uniform_int_distribution<std::size_t> random_payload_size(kMinimumPayloadBytes,
                                                                  kLogMessageSize);

  UniqueFileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
  if (!epoll_fd) {
    throw std::system_error(errno, std::generic_category(), "epoll_create1");
  }
  watch_descriptor(epoll_fd.get(), event_notifications.space_available_fd(), kSpaceAvailableEvent);
  watch_descriptor(epoll_fd.get(), signal_fd, kSignalEvent);
  const auto transfer_start = std::chrono::steady_clock::now();
  const auto record_signal = [&](int signal_number) {
    if (received_signal == 0) {
      received_signal = signal_number;
      consumer_attach_deadline =
          std::chrono::steady_clock::now() + kConsumerAttachGracePeriod;
      if (consumer_attached) {
        consumer_drain_deadline =
            std::chrono::steady_clock::now() + kConsumerDrainGracePeriod;
        consumer_drain_deadline_set = true;
      }
    }
  };
  const auto record_consumer_attachment = [&] {
    consumer_attached = true;
    if (received_signal != 0 && !consumer_drain_deadline_set) {
      consumer_drain_deadline =
          std::chrono::steady_clock::now() + kConsumerDrainGracePeriod;
      consumer_drain_deadline_set = true;
    }
  };

  while (event_count < options.messages) {
    if ((producer_error = server_error.load()) != 0) {
      break;
    }
    if (paused_for_watermark && mapping.queue_size() <= kLogQueueLowWatermark) {
      paused_for_watermark = false;
    }

    std::array<epoll_event, 2> ready_events{};
    const int ready_count = ::epoll_wait(
        epoll_fd.get(), ready_events.data(), ready_events.size(), (paused_for_watermark) ? -1 : 0);
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
      if (event.data.u32 == kSignalEvent) {
        const auto signal_result = consume_signal(signal_fd);
        if (!signal_result) {
          producer_error = -signal_result.error().value();
        } else if (signal_result->has_value()) {
          record_signal(signal_result->value());
        }
        shutdown_requested = true;
        break;
      }
    }
    if (shutdown_requested || producer_error != 0) {
      break;
    }
    for (int i = 0; i < ready_count; ++i) {
      const auto& event = ready_events[i];
      if ((event.events & (EPOLLERR | EPOLLHUP)) || !(event.events & EPOLLIN)) {
        producer_error = -EIO;
        break;
      }
      if (event.data.u32 == kSpaceAvailableEvent) {
        const auto drain_result = drain_eventfd(event_notifications.space_available_fd());
        if (!drain_result) {
          producer_error = -drain_result.error().value();
          break;
        }
        record_consumer_attachment();
      } else {
        producer_error = -EIO;
        break;
      }
    }
    if (producer_error != 0) {
      break;
    }
    if (paused_for_watermark) {
      // Recheck actual occupancy on the next iteration, not just the notification.
      continue;
    }

    int batch_count = kLogQueueBatchSize;

    for (int i = 0; i < batch_count && event_count < options.messages; ++i) {
      if ((producer_error = server_error.load()) != 0) {
        break;
      }
    LogEvent event{};
    event.LogEventId = event_count + 1;
    const std::size_t payload_bytes =
        options.payload_bytes == 0 ? random_payload_size(random_engine) : options.payload_bytes;
    fill_random_payload(event, payload_bytes, random_engine);

    if (mapping.enqueue(event) == EnqueueResult::full) {
      paused_for_watermark = true;
      break;
    }

    ++event_count;
    payload_total_bytes += event.message_length;
  }

    const auto queue_size = mapping.queue_size();
    if (!paused_for_watermark && queue_size >= kLogQueueHighWatermark) {
      paused_for_watermark = true;
    }

    const auto notify_result = event_notifications.notify();
    if (!notify_result) {
        producer_error = -notify_result.error().value();
        break;
    }
  }

  mapping.mark_producer_done();
  const auto notify_result = event_notifications.notify();
  if (producer_error == 0 && !notify_result) {
    producer_error = -notify_result.error().value();
  }

  while (producer_error == 0 && !mapping.consumer_done()) {
    if ((producer_error = server_error.load()) != 0) {
      break;
    }

    int timeout_ms = -1;
    if (received_signal != 0) {
      const auto deadline =
          consumer_attached ? consumer_drain_deadline : consumer_attach_deadline;
      const auto remaining =
          std::chrono::ceil<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
      if (remaining.count() <= 0) {
        shutdown_incomplete = true;
        break;
      }
      timeout_ms = static_cast<int>(remaining.count());
    }

    std::array<epoll_event, 2> ready_events{};
    const int ready_count =
        ::epoll_wait(epoll_fd.get(), ready_events.data(), ready_events.size(), timeout_ms);
    if (ready_count == -1) {
      if (errno == EINTR) {
        continue;
      }
      producer_error = -errno;
      break;
    }
    if (ready_count == 0) {
      continue;
    }

    for (int i = 0; i < ready_count; ++i) {
      if (ready_events[i].data.u32 == kSignalEvent) {
        const auto signal_result = consume_signal(signal_fd);
        if (!signal_result) {
          producer_error = -signal_result.error().value();
        } else if (signal_result->has_value()) {
          record_signal(signal_result->value());
        }
        break;
      }
    }
    if (producer_error != 0) {
      break;
    }

    for (int i = 0; i < ready_count; ++i) {
      if (ready_events[i].data.u32 == kSignalEvent) {
        continue;
      }
      if (ready_events[i].data.u32 == kSpaceAvailableEvent) {
        if ((ready_events[i].events & (EPOLLERR | EPOLLHUP)) ||
            !(ready_events[i].events & EPOLLIN)) {
          producer_error = -EIO;
          break;
        }
        const auto drain_result = drain_eventfd(event_notifications.space_available_fd());
        if (!drain_result) {
          producer_error = -drain_result.error().value();
          break;
        }
        record_consumer_attachment();
      } else {
        producer_error = -EIO;
        break;
      }
    }
  }

  if (received_signal == 0) {
    const auto signal_result = consume_signal(signal_fd);
    if (!signal_result) {
      if (producer_error == 0) {
        producer_error = -signal_result.error().value();
      }
    } else if (signal_result->has_value()) {
      record_signal(signal_result->value());
    }
  }

  const double elapsed_seconds =
      std::chrono::duration<double>(std::chrono::steady_clock::now() - transfer_start).count();
  const double bytes_per_second =
      elapsed_seconds > 0.0 ? static_cast<double>(payload_total_bytes) / elapsed_seconds : 0.0;
  std::cout << "Producer transfer summary:\n"
            << "  total_messages_sent=" << event_count << "\n"
            << "  total_payload_bytes_sent=" << payload_total_bytes << " ["
            << format_byte_size(payload_total_bytes) << "]\n"
            << "  total_elapsed_time_seconds=" << elapsed_seconds << "\n"
            << "  transfer_rate_bytes_per_second=" << std::fixed << std::setprecision(1)
            << bytes_per_second << " [" << format_byte_rate(bytes_per_second) << "]\n";

  if (producer_error != 0 && received_signal == 0) {
    std::cerr << "Producer failed: " << std::strerror(-producer_error) << " (" << producer_error
              << ").\n";
  }
  if (shutdown_incomplete) {
    std::cerr << "Producer shutdown timed out waiting for the consumer to "
              << (consumer_attached ? "complete" : "connect")
              << "; queued messages may not have been written.\n";
  }
  return received_signal != 0 ? 128 + received_signal : (producer_error != 0 ? 1 : 0);
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

void print_producer_help(const char* program) {
  std::cout << "Usage: " << program << " [--messages N] [--payload-bytes N] [--help]\n\n"
            << "  --messages N       Number of random payloads (default "
            << kDefaultTransferMessages << ").\n"
            << "  --payload-bytes N  Fixed payload size, 1-" << kLogMessageSize
            << " (default: random " << kMinimumPayloadBytes << "-" << kLogMessageSize
            << ").\n"
            << "  --help             Show this help.\n";
}

std::expected<void, ParseSizeArgumentError> parse_size_argument(
    int argc, char* argv[], int& index, std::size_t& value) {
  if (index + 1 >= argc) {
    return std::unexpected(ParseSizeArgumentError::missing_value);
  }
  const std::string_view input(argv[index + 1]);
  std::size_t parsed{};
  const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), parsed);
  if (error != std::errc{} || end != input.data() + input.size()) {
    return std::unexpected(ParseSizeArgumentError::invalid_integer);
  }
  value = parsed;
  ++index;
  return {};
}

// Initialize IPC, transfer random payloads, and wait for consumer completion.
int main(int argc, char* argv[]) {
  TransferOptions options;
  bool messages_option_seen = false;
  bool payload_option_seen = false;
  try {
    for (int i = 1; i < argc; ++i) {
      const std::string_view argument(argv[i]);
      if (argument == "--help" || argument == "-h") {
        print_producer_help(argv[0]);
        return 0;
      }
      if (argument == "--messages") {
        if (messages_option_seen) {
          std::cerr << "Option specified more than once: --messages.\n";
          return 2;
        }
        messages_option_seen = true;
        const auto parse_result = parse_size_argument(argc, argv, i, options.messages);
        if (!parse_result) {
          if (parse_result.error() == ParseSizeArgumentError::missing_value) {
            std::cerr << "Missing value for --messages.\n";
          } else {
            std::cerr << "Invalid non-negative integer for --messages: " << argv[i + 1]
                      << ".\n";
          }
          return 2;
        }
      } else if (argument == "--payload-bytes") {
        if (payload_option_seen) {
          std::cerr << "Option specified more than once: --payload-bytes.\n";
          return 2;
        }
        payload_option_seen = true;
        const auto parse_result = parse_size_argument(argc, argv, i, options.payload_bytes);
        if (!parse_result) {
          if (parse_result.error() == ParseSizeArgumentError::missing_value) {
            std::cerr << "Missing value for --payload-bytes.\n";
          } else {
            std::cerr << "Invalid non-negative integer for --payload-bytes: " << argv[i + 1]
                      << ".\n";
          }
          return 2;
        }
      } else {
        std::cerr << "Unknown argument: " << argument << "\n";
        print_producer_help(argv[0]);
        return 2;
      }
    }
    if (options.messages == 0) {
      std::cerr << "--messages must be positive.\n";
      return 2;
    }
    if (options.payload_bytes > kLogMessageSize) {
      std::cerr << "--payload-bytes must not exceed " << kLogMessageSize << ".\n";
      return 2;
    }
    if (payload_option_seen && options.payload_bytes == 0) {
      std::cerr << "Invalid payload size: --payload-bytes must be positive.\n";
      return 2;
    }
    if (options.messages >
        std::numeric_limits<std::uint64_t>::max() / kLogMessageSize) {
      std::cerr << "Total payload bytes exceed the supported range.\n";
      return 2;
    }

    int sig_raw_fd = setup_signals_init();
    UniqueFileDescriptor signal_fd(sig_raw_fd);
    if (!signal_fd) {
      throw std::system_error(errno, std::generic_category(), "signalfd");
    }

    SharedQueueEventFd event_notifications;
    SharedLogQueueMapping<LogEvent, kLogQueueCapacity> mapping;
    mapping.create_shared_memory_ring_buffer();
    std::atomic<int> server_error{0};
    std::jthread notification_server(notification_server_thread_entry,
                                     std::cref(event_notifications), std::ref(server_error));
    mapping.post_ready();

    const int exit_status =
        produce_messages(mapping, event_notifications, signal_fd.get(), server_error, options);
    notification_server.request_stop();
    notification_server.join();
    return exit_status != 0 ? exit_status : (server_error.load() != 0 ? 1 : 0);
  } catch (const std::exception& error) {
    std::cerr << "Producer setup or runtime failed: " << error.what() << '\n';
    return 1;
  }
}
