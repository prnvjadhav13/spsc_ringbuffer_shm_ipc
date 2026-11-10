# Linux SPSC shared-memory ring buffer for IPC

This repository is a small C++23 example of passing fixed-size records between
two separate Linux processes with a single-producer/single-consumer (SPSC) ring
buffer in shared memory. It demonstrates how applications can exchange data
without sending every record through a socket or another kernel-mediated
message path. The shared-memory data path avoids kernel copies between
processes, but this is not end-to-end zero-copy: the producer copies each
record into the ring, the consumer copies it out, and the consumer writes it
to its output stream. `mmap`, `eventfd`, and the initial descriptor handoff
still use Linux kernel facilities.

The sample uses generated log events to show the full producer-to-consumer
flow. It is an educational starting point for Linux applications that need
low-overhead IPC; it is not a benchmark or a guarantee that shared memory will
outperform other IPC for every workload. Measure with representative message
sizes, rates, and consumer behavior before choosing an IPC mechanism for a
production system.

## What the sample does

- The producer creates a POSIX shared-memory object, sizes it for the queue
  layout, and maps it with `mmap(MAP_SHARED)`.
- The consumer maps the same object. Both processes then access the same ring
  storage; log records are not sent through the Unix-domain socket.
- `SpscRingBuffer<T, Capacity>` uses atomic head and tail indices with
  acquire/release ordering. It has one writer on each side and does not use a
  mutex. Its atomic types are required to be lock-free by compile-time checks.
- The fixed-size `LogEvent` record contains an event ID, IP address, severity,
  and message. The example queue holds 2,048 records.
- After a successful enqueue, the producer signals a nonblocking `eventfd`.
  The consumer waits for that descriptor with level-triggered `epoll`, drains
  the notification, then dequeues records until the ring is empty. Eventfd
  notifications can coalesce; the ring's state, not the eventfd count, is the
  source of truth for available records.
- The consumer prints records and appends them to `consumer_logs.out` by
  default. Pass an output path as the consumer's optional argument to select a
  different file.
- When queue occupancy reaches the 90% high watermark, the producer pauses.
  As the consumer drains the queue through the 40% low watermark, it signals a
  second eventfd. The producer waits for that notification with `epoll` and
  rechecks queue occupancy before resuming. This high/low watermark policy
  applies backpressure instead of allowing an unbounded queue to grow.
- The producer also watches a `signalfd` and a one-shot 20-second timer in its
  event loop. It stops on SIGINT, SIGTERM, or timeout, marks the queue as
  complete, and notifies the consumer so it can drain remaining records and
  exit. The consumer handles SIGINT/SIGTERM through its own `signalfd`, drains
  queued records, flushes the output file, and then exits.

The producer creates both eventfds. For initial setup only, it passes them to
the consumer using `SCM_RIGHTS` over a local abstract Unix-domain socket. This
small control-plane handoff does not carry log records; the data plane remains
the shared-memory ring. A larger event-driven producer can register the
space-available eventfd alongside its other descriptors in the same `epoll`
loop. In this sample, the producer has no separate input or TLS work to service
while paused, so it waits for queue space, signals, or its timer.

## Run it

Linux, a C++23-capable compiler (the Makefile defaults to `g++`), GNU Make, and
POSIX threads are required.

Build optimized binaries:

```sh
make release
```

Or build binaries with AddressSanitizer and UndefinedBehaviorSanitizer for
debugging:

```sh
make debug
```

Start the consumer and producer in separate terminals, from the repository
root. The consumer waits for the producer to create and initialize the queue:

```sh
./build/release/consumer
./build/release/producer
```

The default consumer output file is `consumer_logs.out` in its current working
directory. To choose another file:

```sh
./build/release/consumer /tmp/sample-logs.out
```

The debug binaries are `build/debug/consumer` and `build/debug/producer`; run
both endpoints from the same build type. Sanitizers add runtime overhead and
are intended for debugging, not performance measurements. Build both sets
with `make` or just one set with `make release` / `make debug`. Remove generated
binaries with `make clean`.

## Code map

- `spsc_ringbuffer.hpp` — fixed-capacity SPSC ring buffer and an alternate
  implementation retained for comparison.
- `shared_queue_storage.hpp` — in-place shared-memory layout, compatibility
  metadata, ring, and producer-completion flag.
- `shm_log_queue_mapping.hpp` / `shm_log_queue_mapping.cpp` — shared-memory
  creation, mapping, readiness, role-checked queue operations, and cleanup.
- `shm_queue_eventfd.hpp` / `shm_queue_eventfd.cpp` — eventfd notifications,
  eventfd descriptor handoff, and move-only file-descriptor ownership.
- `shm_Prod_QueueOps.cpp` — sample event producer and its `epoll` loop.
- `shm_Consum_QueueOps.cpp` — sample consumer, `epoll` loop, and file output.
- `log_queue_config.hpp` — `LogEvent`, queue capacity, watermarks, and IPC
  names.
- `shm_ringbuffer_tests.cpp` — standalone ring-buffer test/example source.

## Adapting the queue

The ring buffer is for one producer and one consumer only. Do not have multiple
threads or processes enqueue or dequeue concurrently without adding separate
synchronization or changing the design. Messages must be fixed-size and
trivially copyable so that both processes can use the same in-place layout.

The producer and consumer must be built with matching message types and queue
capacity. The consumer checks the shared-memory size and metadata before using
the ring. Update `kSharedQueueVersion` when a layout change is not detected by
the size, alignment, or other existing metadata checks. The configured
template instantiation is in `shm_log_queue_mapping.cpp`; a different message
type or capacity needs a corresponding explicit instantiation.

The sample uses fixed shared-memory, semaphore, and abstract socket names, and
is intended for one producer/consumer pair at a time. The producer unlinks its
named shared-memory and semaphore resources during normal cleanup; a forced
termination can leave names behind and interfere with a later run. The IPC
setup, `eventfd`, `signalfd`, and `epoll` implementation is Linux-specific.
