#ifndef _LOG_QUEUE_CONFIG_HPP_
#define _LOG_QUEUE_CONFIG_HPP_

#include "shared_queue_storage.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

inline constexpr char kLogQueueSharedMemoryName[] = "/shm_log_ring_buffer";
inline constexpr char kLogQueueReadySemaphoreName[] = "/sem_log_ring_buffer_ready";
inline constexpr std::size_t kLogQueueCapacity = 2 * 1024;
inline constexpr std::size_t kLogQueueHighWatermark =
    (kLogQueueCapacity * 90) / 100;
inline constexpr std::size_t kLogQueueLowWatermark =
    (kLogQueueCapacity * 40) / 100;
inline constexpr const char kConsumerFilePath[] = "consumer_logs.out";

inline constexpr std::size_t kLogIpSize = 129;
inline constexpr std::size_t kLogMessageSize = 1024;

enum class Level : std::uint32_t {
    DEBUG = 0,
    INFO,
    WARN,
    ERROR,
    CRITICAL,
    FATAL
};

struct LogEvent {
    std::size_t LogEventId{};
    char ip[kLogIpSize]{};
    Level level{Level::INFO};
    char message[kLogMessageSize]{};
};

static_assert(std::is_trivially_copyable_v<LogEvent>,
              "LogEvent must be safe to copy through shared memory");


#endif
