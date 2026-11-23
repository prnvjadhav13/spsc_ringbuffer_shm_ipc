// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "log_queue_config.hpp"
#include "shm_log_queue_mapping.hpp"
#include "shm_queue_eventfd.hpp"
#include "transfer_stats.hpp"

namespace {

constexpr std::uint32_t kEpollEventQueue = 1;
constexpr std::uint32_t kEpollEventSignal = 2;
constexpr char kHexDigits[] = "0123456789abcdef";
constexpr std::size_t kPayloadWriterQueueCapacity = kLogQueueCapacity;
constexpr std::size_t kSignalCheckBatchSize = 64;

enum class QueuePushResult { pushed, closed };

std::optional<int> read_signal(int signal_fd) {
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
      return std::nullopt;
    }
    if (bytes_read == -1) {
      throw std::system_error(errno, std::generic_category(), "read signalfd");
    }
    throw std::runtime_error("short read from signalfd");
  }
}

void write_payload_hex(std::ostream& output, const LogEvent& message,
                       std::string& encoded_payload) {
  encoded_payload.clear();
  encoded_payload.append(std::to_string(message.LogEventId));
  encoded_payload.push_back(':');
  encoded_payload.append(std::to_string(message.message_length));
  encoded_payload.append(": [");
  for (std::size_t i = 0; i < message.message_length; ++i) {
    const unsigned char byte = std::to_integer<unsigned char>(message.payload[i]);
    encoded_payload.push_back(kHexDigits[byte >> 4]);
    encoded_payload.push_back(kHexDigits[byte & 0x0F]);
  }
  encoded_payload.append("]\n");
  output.write(encoded_payload.data(), static_cast<std::streamsize>(encoded_payload.size()));
}

void write_payload_binary(std::ostream& output, const LogEvent& message) {
  output.write(reinterpret_cast<const char*>(message.payload),
               static_cast<std::streamsize>(message.message_length));
}

class BoundedLogEventQueue {
 public:
  explicit BoundedLogEventQueue(std::size_t capacity) : slots_(capacity) {}

  QueuePushResult push(const LogEvent& message) {
    std::unique_lock lock(mutex_);
    if (closed_) {
      return QueuePushResult::closed;
    }
    queue_not_full_.wait(lock, [this] { return count_ < slots_.size() || closed_; });
    if (closed_) {
      return QueuePushResult::closed;
    }

    slots_[write_index_] = message;
    write_index_ = (write_index_ + 1) % slots_.size();
    ++count_;
    lock.unlock();
    queue_not_empty_.notify_one();
    return QueuePushResult::pushed;
  }

  QueuePushResult push(LogEvent&& message) {
    std::unique_lock lock(mutex_);
    if (closed_) {
      return QueuePushResult::closed;
    }
    queue_not_full_.wait(lock, [this] { return count_ < slots_.size() || closed_; });
    if (closed_) {
      return QueuePushResult::closed;
    }

    slots_[write_index_] = std::move(message);
    write_index_ = (write_index_ + 1) % slots_.size();
    ++count_;
    lock.unlock();
    queue_not_empty_.notify_one();
    return QueuePushResult::pushed;
  }

  std::optional<std::reference_wrapper<const LogEvent>> front() {
    std::unique_lock lock(mutex_);
    queue_not_empty_.wait(lock, [this] { return count_ != 0 || closed_; });
    // Keep this slot occupied until pop_front while the writer reads it unlocked.
    if (count_ == 0) {
      return std::nullopt;
    }
    return std::cref(slots_[read_index_]);
  }

  void pop_front() {
    {
      std::lock_guard lock(mutex_);
      read_index_ = (read_index_ + 1) % slots_.size();
      --count_;
    }
    queue_not_full_.notify_one();
  }

  void close() {
    {
      std::lock_guard lock(mutex_);
      closed_ = true;
    }
    queue_not_empty_.notify_all();
    queue_not_full_.notify_all();
  }

  bool is_empty() {
    std::lock_guard lock(mutex_);
    return count_ == 0;
  }

 private:
  std::vector<LogEvent> slots_;
  std::mutex mutex_;
  std::condition_variable queue_not_empty_;
  std::condition_variable queue_not_full_;
  std::size_t read_index_{};
  std::size_t write_index_{};
  std::size_t count_{};
  bool closed_{};
};

class PayloadWriterThread {
 public:
  PayloadWriterThread(BoundedLogEventQueue& queue, std::thread thread)
      : queue_(queue), thread_(std::move(thread)) {}

  ~PayloadWriterThread() { finish(); }

  void finish() {
    queue_.close();
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  BoundedLogEventQueue& queue_;
  std::thread thread_;
};

void payload_writer_thread(BoundedLogEventQueue& queue, std::ostream& output,
                           const char* output_path, std::string& encoded_payload,
                           bool binary_output, std::atomic<bool>& output_ok,
                           std::exception_ptr& writer_error) {
  bool writer_failed = false;
  while (const auto message = queue.front()) {
    if (!writer_failed && output_ok.load(std::memory_order_relaxed)) {
      try {
        if (binary_output) {
          write_payload_binary(output, message->get());
        } else {
          write_payload_hex(output, message->get(), encoded_payload);
        }
        if (!output) {
          throw std::runtime_error(std::string("Failed while writing payload data to: ") +
                                   output_path);
        }
      } catch (...) {
        writer_error = std::current_exception();
        writer_failed = true;
        output_ok.store(false, std::memory_order_relaxed);
      }
    }
    queue.pop_front();
  }

  if (!writer_failed) {
    try {
      output.flush();
      if (!output) {
        throw std::runtime_error(std::string("Failed to flush payload output file: ") +
                                 output_path);
      }
    } catch (...) {
      writer_error = std::current_exception();
      output_ok.store(false, std::memory_order_relaxed);
    }
  }
}

void print_consumer_help(const char* program) {
  std::cout << "Usage: " << program << " --file-path output-file [--help]\n\n"
            << "  --file-path output-file  Required destination for received payloads "
               "(text hex format by default).\n"
            << "  --binary-output         Write raw payload bytes concatenated, without "
               "record boundaries.\n"
            << "  --help                   Show this help.\n";
}

}  // namespace

// Drain payload messages into a bounded writer queue and report transfer totals.
int main(int argc, char* argv[]) {
  const char* output_path = nullptr;
  bool binary_output = false;
  for (int i = 1; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument == "--help" || argument == "-h") {
      print_consumer_help(argv[0]);
      return 0;
    }
    if (argument == "--binary-output") {
      if (binary_output) {
        std::cerr << "Option specified more than once: --binary-output.\n";
        return 2;
      }
      binary_output = true;
      continue;
    }
    if (argument != "--file-path") {
      std::cerr << "Unknown argument: " << argument << '\n';
      print_consumer_help(argv[0]);
      return 2;
    }
    if (output_path != nullptr) {
      std::cerr << "Option specified more than once: --file-path.\n";
      return 2;
    }
    if (i + 1 >= argc || std::string_view(argv[i + 1]).starts_with('-')) {
      std::cerr << "Missing value for --file-path.\n";
      return 2;
    }
    output_path = argv[++i];
  }
  if (output_path == nullptr) {
    std::cerr << "Missing required option: --file-path.\n";
    return 2;
  }

  sigset_t stop_signals;
  sigemptyset(&stop_signals);
  sigaddset(&stop_signals, SIGINT);
  sigaddset(&stop_signals, SIGTERM);
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

  const auto output_mode = std::ios::out | std::ios::trunc |
                           (binary_output ? std::ios::binary : std::ios::openmode{});
  std::ofstream output_file(output_path, output_mode);
  if (!output_file) {
    std::cerr << "Could not open payload output file: " << output_path << '\n';
    return 1;
  }

  try {
    SharedLogQueueMapping<LogEvent, kLogQueueCapacity> mapping;
    if (mapping.open_shared_memory_ring_buffer(signal_fd.get()) ==
        QueueStartupResult::interrupted) {
      const auto signal = read_signal(signal_fd.get());
      return signal ? 128 + *signal : 1;
    }
    if (mapping.wait_until_ready(signal_fd.get()) == QueueStartupResult::interrupted) {
      const auto signal = read_signal(signal_fd.get());
      return signal ? 128 + *signal : 1;
    }

    auto notification_fds = SharedQueueEventFd::receive(signal_fd.get());
    if (!notification_fds.queue_ready || !notification_fds.space_available) {
      const auto signal = read_signal(signal_fd.get());
      if (signal) {
        return 128 + *signal;
      }
      std::cerr << "Producer notification handoff did not complete before timeout.\n";
      return 1;
    }

    UniqueFileDescriptor epoll_fd(::epoll_create1(EPOLL_CLOEXEC));
    if (!epoll_fd) {
      throw std::system_error(errno, std::generic_category(), "epoll_create1");
    }
    epoll_event queue_event{};
    queue_event.events = EPOLLIN;
    queue_event.data.u32 = kEpollEventQueue;
    if (::epoll_ctl(epoll_fd.get(), EPOLL_CTL_ADD, notification_fds.queue_ready.get(),
                    &queue_event) == -1) {
      throw std::system_error(errno, std::generic_category(), "epoll_ctl add queue eventfd");
    }
    epoll_event signal_event{};
    signal_event.events = EPOLLIN;
    signal_event.data.u32 = kEpollEventSignal;
    if (::epoll_ctl(epoll_fd.get(), EPOLL_CTL_ADD, signal_fd.get(), &signal_event) == -1) {
      throw std::system_error(errno, std::generic_category(), "epoll_ctl add signalfd");
    }

    BoundedLogEventQueue writer_queue(kPayloadWriterQueueCapacity);
    std::string encoded_payload;
    if (!binary_output) {
      encoded_payload.reserve(2 * kLogMessageSize + 64);
    }
    std::atomic<bool> output_ok{true};
    std::exception_ptr writer_error;
    std::thread writer(payload_writer_thread, std::ref(writer_queue), std::ref(output_file),
                       output_path, std::ref(encoded_payload), binary_output,
                       std::ref(output_ok), std::ref(writer_error));
    PayloadWriterThread writer_thread(writer_queue, std::move(writer));
    const auto attach_notify_result = notify_eventfd(notification_fds.space_available.get());
    if (!attach_notify_result) {
      throw std::system_error(attach_notify_result.error(),
                              "notify producer of consumer attachment");
    }

    const auto transfer_start = std::chrono::steady_clock::now();
    std::size_t messages_received = 0;
    std::uint64_t payload_total_bytes = 0;
    int received_signal = 0;
    std::chrono::steady_clock::time_point ring_drain_end;

    while (true) {
      std::array<epoll_event, 2> ready_events{};
      mapping.mark_consumer_waiting();
      const int ready_count =
          ::epoll_wait(epoll_fd.get(), ready_events.data(), ready_events.size(), -1);
      if (ready_count == -1) {
        if (errno == EINTR) {
          continue;
        }
        throw std::system_error(errno, std::generic_category(), "epoll_wait");
      }
      mapping.reset_consumer_waiting();

      bool queue_notified = false;
      for (int i = 0; i < ready_count; ++i) {
        if (ready_events[i].data.u32 == kEpollEventQueue) {
          queue_notified = true;
        } else if (ready_events[i].data.u32 == kEpollEventSignal) {
          if (const auto signal = read_signal(signal_fd.get());
              signal && received_signal == 0) {
            received_signal = *signal;
          }
        }
      }
      if (received_signal != 0) {
        ring_drain_end = std::chrono::steady_clock::now();
        break;
      }
      if (!queue_notified) {
        continue;
      }

      const auto drain_result = drain_eventfd(notification_fds.queue_ready.get());
      if (!drain_result) {
        throw std::system_error(drain_result.error(), "drain queue notification");
      }

      while (const auto queue_size = mapping.queue_size()) {
       // const std::size_t queue_size = mapping.queue_size();
        const LogEvent& message = mapping.front();

        if (message.message_length == 0 || message.message_length > kLogMessageSize) {
          std::cerr << "Received invalid payload length: " << message.message_length << ".\n";
          output_ok.store(false, std::memory_order_relaxed);
        } else if (message.message_length >
                   std::numeric_limits<std::uint64_t>::max() - payload_total_bytes) {
          std::cerr << "Payload byte total exceeds the supported range.\n";
          output_ok.store(false, std::memory_order_relaxed);
        } else {
          ++messages_received;
          payload_total_bytes += message.message_length;
          if (writer_queue.push(message) == QueuePushResult::closed) {
            throw std::runtime_error("Payload writer queue closed before transfer completed");
          }
        }

        if (queue_size != 0 && queue_size <= kLogQueueLowWatermark) {
          const auto notify_result = notify_eventfd(notification_fds.space_available.get());
          if (!notify_result) {
            std::cerr << "Space-available notification failed: "
                      << notify_result.error().message() << ".\n";
            output_ok.store(false, std::memory_order_relaxed);
          }
        }

        mapping.pop_front();
      }

      if (mapping.producer_done() && mapping.queue_size() == 0) {
        ring_drain_end = std::chrono::steady_clock::now();
        mapping.mark_consumer_done();
        const auto notify_result = notify_eventfd(notification_fds.space_available.get());
        if (!notify_result) {
          std::cerr << "Consumer-completion notification failed: "
                    << notify_result.error().message() << ".\n";
          output_ok.store(false, std::memory_order_relaxed);
        }
        break;
      }
    }

    writer_thread.finish();

    const double elapsed_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - transfer_start).count();
    const double ring_drain_elapsed_seconds =
        std::chrono::duration<double>(ring_drain_end - transfer_start).count();
    const double bytes_per_second =
        elapsed_seconds > 0.0 ? static_cast<double>(payload_total_bytes) / elapsed_seconds : 0.0;
    const double ring_drain_bytes_per_second =
        ring_drain_elapsed_seconds > 0.0
            ? static_cast<double>(payload_total_bytes) / ring_drain_elapsed_seconds
            : 0.0;
    if (writer_error) {
      try {
        std::rethrow_exception(writer_error);
      } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
      } catch (...) {
        std::cerr << "Payload writer failed with an unknown error.\n";
      }
    }
    std::cout << "Consumer transfer summary:\n"
              << "  total_messages_received=" << messages_received << "\n"
              << "  total_payload_bytes_received=" << payload_total_bytes << " ["
              << format_byte_size(payload_total_bytes) << "]\n"
              << "  ring_drain_elapsed_seconds=" << ring_drain_elapsed_seconds << "\n"
              << "  ring_drain_transfer_rate_bytes_per_second=" << std::fixed
              << std::setprecision(1) << ring_drain_bytes_per_second << " ["
              << format_byte_rate(ring_drain_bytes_per_second) << "]\n"
              << std::defaultfloat << std::setprecision(6)
              << "  total_elapsed_time_seconds=" << elapsed_seconds << "\n"
              << "  transfer_rate_bytes_per_second=" << std::fixed << std::setprecision(1)
              << bytes_per_second << " [" << format_byte_rate(bytes_per_second) << "]\n";

    return output_ok.load(std::memory_order_relaxed)
               ? (received_signal == 0 ? 0 : 128 + received_signal)
               : 1;
  } catch (const std::exception& error) {
    std::cerr << "Consumer setup or runtime failed: " << error.what() << '\n';
    return 1;
  }
}
