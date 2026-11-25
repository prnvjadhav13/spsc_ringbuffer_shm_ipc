// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

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
    inline constexpr std::size_t kLogQueueBatchSize = kLogQueueCapacity / 4;
inline constexpr std::size_t kDefaultTransferMessages = 1'000;

inline constexpr std::size_t kMinimumPayloadBytes = 1024;
inline constexpr std::size_t kLogMessageSize = 2024; // 2 KiB
static_assert(kMinimumPayloadBytes <= kLogMessageSize);

struct LogEvent {
    std::size_t LogEventId{};
    std::size_t message_length{0};
    std::byte payload[kLogMessageSize]{};
};

struct TransferOptions {
    std::size_t messages{kDefaultTransferMessages};
    std::size_t payload_bytes{};
};

static_assert(std::is_trivially_copyable_v<LogEvent>,
              "LogEvent must be safe to copy through shared memory");


#endif
