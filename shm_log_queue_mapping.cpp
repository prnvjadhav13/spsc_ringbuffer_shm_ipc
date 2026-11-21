// Copyright (c) 2026 Ahmad Jadhav. All rights reserved.
// This software is provided solely for educational purposes.
// See the repository LICENSE file for reuse terms.

#include "shm_log_queue_mapping.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "log_queue_config.hpp"
#include "shm_signal_wait.hpp"

namespace {

template <typename T, std::size_t Capacity>
off_t checked_storage_file_size() {
  constexpr std::size_t storage_size = sizeof(SharedQueueStorage<T, Capacity>);
  if (static_cast<std::uintmax_t>(storage_size) >
      static_cast<std::uintmax_t>(std::numeric_limits<off_t>::max())) {
    throw std::length_error("Shared-memory queue size does not fit in off_t");
  }
  return static_cast<off_t>(storage_size);
}

void unlink_if_present(int (*unlink_resource)(const char*), const char* name,
                       const char* operation) {
  if (unlink_resource(name) == -1 && errno != ENOENT) {
    throw std::system_error(errno, std::generic_category(), operation);
  }
}

}  // namespace

// Release this mapping and unlink IPC names only when this instance owns them.
template <typename T, std::size_t Capacity>
SharedLogQueueMapping<T, Capacity>::~SharedLogQueueMapping() noexcept {
  reset();
}

// Take all mapping resources and ownership flags from another instance.
template <typename T, std::size_t Capacity>
SharedLogQueueMapping<T, Capacity>::SharedLogQueueMapping(SharedLogQueueMapping&& other) noexcept {
  take_resources(other);
}

// Release current resources before taking ownership from the moved instance.
template <typename T, std::size_t Capacity>
SharedLogQueueMapping<T, Capacity>& SharedLogQueueMapping<T, Capacity>::operator=(
    SharedLogQueueMapping&& other) noexcept {
  if (this != &other) {
    reset();
    take_resources(other);
  }
  return *this;
}

// Create the producer-owned shared memory and readiness semaphore, then construct queue storage.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::create_shared_memory_ring_buffer() {
  if (mapping_ != nullptr || fd_ != -1 || ready_ != SEM_FAILED) {
    throw std::logic_error("SharedLogQueueMapping is already open");
  }
  const off_t storage_file_size = checked_storage_file_size<T, Capacity>();

  // The producer binds its exclusive notification socket before calling here.
  // If that succeeded, no live producer owns these fixed-name resources.
  unlink_if_present(shm_unlink, kLogQueueSharedMemoryName, "shm_unlink stale queue");
  unlink_if_present(sem_unlink, kLogQueueReadySemaphoreName, "sem_unlink stale readiness");

  ready_ = sem_open(kLogQueueReadySemaphoreName, O_CREAT | O_EXCL, 0660, 0);
  if (ready_ == SEM_FAILED) {
    throw_errno("sem_open");
  }
  owns_sem_ready_name_ = true;

  fd_ = shm_open(kLogQueueSharedMemoryName, O_CREAT | O_EXCL | O_RDWR, 0660);
  if (fd_ == -1) {
    throw_errno("shm_open");
  }
  owns_shared_memory_name_ = true;

  if (ftruncate(fd_, storage_file_size) == -1) {
    throw_errno("ftruncate");
  }

  void* address = mmap(nullptr, sizeof(SharedQueueStorage<T, Capacity>), PROT_READ | PROT_WRITE,
                       MAP_SHARED, fd_, 0);
  if (address == MAP_FAILED) {
    throw_errno("mmap");
  }

  mapping_ = address;
  shared_ = ::new (mapping_) SharedQueueStorage<T, Capacity>{};
  role_ = Role::producer;
}

// Wait for shared memory and validate its layout; report when startup is interrupted.
template <typename T, std::size_t Capacity>
QueueStartupResult SharedLogQueueMapping<T, Capacity>::open_shared_memory_ring_buffer(
    int signal_fd) {
  if (mapping_ != nullptr || fd_ != -1 || ready_ != SEM_FAILED) {
    throw std::logic_error("SharedLogQueueMapping is already open");
  }
  const off_t storage_file_size = checked_storage_file_size<T, Capacity>();

  for (;;) {
    fd_ = shm_open(kLogQueueSharedMemoryName, O_RDWR, 0);
    if (fd_ != -1) {
      break;
    }
    if (errno != ENOENT) {
      throw_errno("shm_open");
    }
    if (wait_for_signal_or_timeout(signal_fd, std::chrono::milliseconds(100)) ==
        SignalWaitResult::signal_received) {
      return QueueStartupResult::interrupted;
    }
  }

  ready_ = sem_open(kLogQueueReadySemaphoreName, 0);
  if (ready_ == SEM_FAILED) {
    throw_errno("sem_open");
  }
  // Acquire initialization before reading storage metadata. Restore the token
  // so a later consumer can attach too; sem_getvalue alone does not acquire it.
  for (;;) {
    if (sem_trywait(ready_) == 0) {
      if (sem_post(ready_) == -1) {
        throw_errno("sem_post readiness");
      }
      break;
    }
    if (errno != EAGAIN && errno != EINTR) {
      throw_errno("sem_trywait readiness");
    }
    if (wait_for_signal_or_timeout(signal_fd, std::chrono::milliseconds(50)) ==
        SignalWaitResult::signal_received) {
      return QueueStartupResult::interrupted;
    }
  }

  struct stat info{};
  for (;;) {
    if (fstat(fd_, &info) == -1) {
      throw_errno("fstat");
    }
    if (info.st_size == storage_file_size) {
      break;
    }
    if (info.st_size != 0) {
      throw std::runtime_error("Shared-memory segment size does not match queue configuration");
    }
    if (wait_for_signal_or_timeout(signal_fd, std::chrono::milliseconds(50)) ==
        SignalWaitResult::signal_received) {
      return QueueStartupResult::interrupted;
    }
  }

  void* address = mmap(nullptr, sizeof(SharedQueueStorage<T, Capacity>), PROT_READ | PROT_WRITE,
                       MAP_SHARED, fd_, 0);
  if (address == MAP_FAILED) {
    throw_errno("mmap");
  }

  mapping_ = address;
  shared_ = static_cast<SharedQueueStorage<T, Capacity>*>(mapping_);
  if (shared_->magic != kSharedQueueMagic || shared_->version != kSharedQueueVersion ||
      shared_->capacity != Capacity || shared_->message_size != sizeof(T) ||
      shared_->message_alignment != alignof(T)) {
    throw std::runtime_error("Shared-memory queue metadata does not match queue configuration");
  }
  role_ = Role::consumer;
  return QueueStartupResult::ready;
}

// Enqueue as the producer and distinguish a full queue from a successful enqueue.
template <typename T, std::size_t Capacity>
EnqueueResult SharedLogQueueMapping<T, Capacity>::enqueue(const T& message) {
  require_role(Role::producer, "enqueue");
  return shared_->ring.EnQueue(message);
}

// Dequeue as the consumer and distinguish an empty queue from a successful dequeue.
template <typename T, std::size_t Capacity>
DequeueResult SharedLogQueueMapping<T, Capacity>::dequeue(T& message) {
  require_role(Role::consumer, "dequeue");
  return shared_->ring.DeQueue(message);
}

// Expose the oldest consumer-side element by reference without copying it.
template <typename T, std::size_t Capacity>
T& SharedLogQueueMapping<T, Capacity>::front() {
  require_role(Role::consumer, "front");
  return shared_->ring.front();
}

// Expose the oldest consumer-side element by const reference without copying it.
template <typename T, std::size_t Capacity>
const T& SharedLogQueueMapping<T, Capacity>::front() const {
  require_role(Role::consumer, "front");
  return shared_->ring.front();
}

// Advance past the oldest element without copying or moving its payload.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::pop_front() {
  require_role(Role::consumer, "pop front");
  shared_->ring.pop_front();
}

// Read the current queue occupancy after verifying that a mapping is open.
template <typename T, std::size_t Capacity>
std::size_t SharedLogQueueMapping<T, Capacity>::queue_size() const {
  if (role_ == Role::none || shared_ == nullptr) {
    throw std::logic_error("Shared-memory queue is not open");
  }
  return shared_->ring.Size();
}

// Publish producer completion with release ordering for the consumer to observe.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::mark_producer_done() {
  require_role(Role::producer, "mark producer done");
  shared_->producer_done.store(true, std::memory_order_release);
}

// Read producer completion with acquire ordering from the consumer side.
template <typename T, std::size_t Capacity>
bool SharedLogQueueMapping<T, Capacity>::producer_done() const {
  require_role(Role::consumer, "read producer completion state");
  return shared_->producer_done.load(std::memory_order_acquire);
}

// Publish that the consumer drained and flushed all queued messages.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::mark_consumer_done() {
  require_role(Role::consumer, "mark consumer done");
  shared_->consumer_done.store(true, std::memory_order_release);
}

// Read consumer completion after verifying the producer mapping is open.
template <typename T, std::size_t Capacity>
bool SharedLogQueueMapping<T, Capacity>::consumer_done() const {
  require_role(Role::producer, "read consumer completion state");
  return shared_->consumer_done.load(std::memory_order_acquire);
}

// Publish producer readiness by posting the initialization semaphore.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::post_ready() const {
  require_role(Role::producer, "post queue readiness");
  if (ready_ == SEM_FAILED) {
    throw std::logic_error("Shared-memory semaphore is not open");
  }
  if (owns_sem_ready_name_ && sem_post(ready_) == -1) {
    throw_errno("sem_post");
  }
}

// Poll readiness without consuming it; report when a shutdown signal interrupts startup.
template <typename T, std::size_t Capacity>
QueueStartupResult SharedLogQueueMapping<T, Capacity>::wait_until_ready(int signal_fd) const {
  require_role(Role::consumer, "wait for queue readiness");
  if (ready_ == SEM_FAILED) {
    throw std::logic_error("Shared-memory semaphore is not open");
  }
  // Peek instead of waiting, so one consumer cannot consume readiness
  // and prevent a restarted consumer from attaching.
  for (;;) {
    int value = 0;
    if (sem_getvalue(ready_, &value) == -1) {
      throw_errno("sem_getvalue");
    }
    if (value > 0) {
      return QueueStartupResult::ready;
    }
    if (wait_for_signal_or_timeout(signal_fd, std::chrono::milliseconds(50)) ==
        SignalWaitResult::signal_received) {
      return QueueStartupResult::interrupted;
    }
  }
}

// Reject operations unless the mapping is open with the required producer or consumer role.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::require_role(Role expected, const char* operation) const {
  if (role_ != expected || shared_ == nullptr) {
    throw std::logic_error(std::string("Invalid queue role for ") + operation);
  }
}

// Throw a system_error describing the operation and current errno.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::throw_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

// Transfer all IPC handles and cleanup responsibilities, leaving the source empty.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::take_resources(SharedLogQueueMapping& other) noexcept {
  fd_ = std::exchange(other.fd_, -1);
  mapping_ = std::exchange(other.mapping_, nullptr);
  shared_ = std::exchange(other.shared_, nullptr);
  ready_ = std::exchange(other.ready_, SEM_FAILED);
  owns_shared_memory_name_ = std::exchange(other.owns_shared_memory_name_, false);
  owns_sem_ready_name_ = std::exchange(other.owns_sem_ready_name_, false);
  role_ = std::exchange(other.role_, Role::none);
}

// Unmap and close resources, unlink owned IPC names, and restore the empty state.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::reset() noexcept {
  if (mapping_ != nullptr && munmap(mapping_, sizeof(SharedQueueStorage<T, Capacity>)) == -1) {
    report_cleanup_error("munmap");
  }
  if (fd_ != -1 && close(fd_) == -1) {
    report_cleanup_error("close shared memory");
  }
  if (ready_ != SEM_FAILED && sem_close(ready_) == -1) {
    report_cleanup_error("sem_close");
  }
  if (owns_shared_memory_name_ && shm_unlink(kLogQueueSharedMemoryName) == -1) {
    report_cleanup_error("shm_unlink");
  }
  if (owns_sem_ready_name_ && sem_unlink(kLogQueueReadySemaphoreName) == -1) {
    report_cleanup_error("sem_unlink");
  }
  fd_ = -1;
  mapping_ = nullptr;
  shared_ = nullptr;
  ready_ = SEM_FAILED;
  owns_shared_memory_name_ = false;
  owns_sem_ready_name_ = false;
  role_ = Role::none;
}

// Log cleanup failures without throwing from resource-release paths.
template <typename T, std::size_t Capacity>
void SharedLogQueueMapping<T, Capacity>::report_cleanup_error(const char* operation) noexcept {
  const int error = errno;
  std::fprintf(stderr, "%s during shared-memory cleanup: %s\n", operation, std::strerror(error));
}

template class SharedLogQueueMapping<LogEvent, kLogQueueCapacity>;
