# Linux SPSC shared-memory ring buffer for IPC

## Project overview

**Project:** Linux Shared-Memory IPC Framework Using Lock-Free SPSC Ring Buffer  

This C++ sample demonstrates inter-process communication (IPC) between
independent Linux processes using a lock-free, shared-memory SPSC ring buffer.
It combines POSIX shared memory mapped with `mmap(MAP_SHARED)` for the message
data path with `eventfd` notifications and `epoll` waits. This avoids routing
each message through a Unix-domain socket or another kernel-mediated
message-transport path, and avoids kernel-mediated copying of the message
payload between the processes. It is not end-to-end zero-copy: the producer
copies each message into the ring and the consumer copies it out.

The ring buffer is templated on message type and capacity. Messages must be
fixed-size and trivially copyable so both processes can interpret the same
shared-memory layout. The ring's atomic head and tail indices are separated
onto cache lines to reduce false sharing; acquire/release ordering coordinates
the single producer and single consumer without a mutex on the ring data path.
In this repository, the process-level example uses its configured payload
record and capacity; adapting the mapping to another type or capacity requires
matching configuration and template instantiation in both processes.

`eventfd` provides cross-process wakeups, while `epoll` lets the endpoints wait
for notifications and other registered events rather than busy-polling. A
second eventfd carries space-available notifications for high/low watermark
backpressure. Unix-domain sockets are used only during setup to pass the
eventfd descriptors with `SCM_RIGHTS`; message data stays in shared memory.
The sample also demonstrates signal handling, completion notification, and
orderly resource cleanup.

### Key technical highlights

1. A fixed-capacity lock-free SPSC ring with compile-time checks for message
   constraints and lock-free atomics.
2. A shared-memory data path established with POSIX shared memory and
   `mmap(MAP_SHARED)`.
3. Event-driven notifications using `eventfd`, `epoll`, and `signalfd` for
   wakeups, event handling, and graceful signal processing.
4. Producer-consumer flow control with 90% high and 40% low watermarks to
   apply backpressure.
5. `SCM_RIGHTS` descriptor passing over a Unix-domain socket for notification
   setup, without using that socket to carry message payloads.
6. Linux-native IPC lifecycle handling, including queue completion signaling
   and cleanup of resources owned by the producer.

The sample transfers random payload messages to show the full
producer-to-consumer flow. It is an educational starting point for Linux
applications that need low-overhead IPC; it is not a benchmark or a guarantee
that shared memory will outperform other IPC for every workload. Measure with
representative message sizes, rates, and consumer behavior before choosing an
IPC mechanism for a production system.

## What the sample does

- The producer creates a POSIX shared-memory object, sizes it for the queue
  layout, and maps it with `mmap(MAP_SHARED)`.
- The consumer maps the same object. Both processes then access the same ring
  storage; payload messages are not sent through the Unix-domain socket.
- `SpscRingBuffer<T, Capacity>` uses atomic head and tail indices with
  acquire/release ordering. It has one writer on each side and does not use a
  mutex. Its atomic types are required to be lock-free by compile-time checks.
- Each fixed-size queue record contains a payload length and up to 2,024 bytes
  of random payload data. The example queue holds 2,048 cache-line-padded
  records.
- The producer batches queue-ready notifications, then signals a nonblocking
  `eventfd`. The consumer waits for that descriptor with level-triggered
  `epoll`, drains the notification, then dequeues records until the ring is
  empty. Eventfd notifications can coalesce; the ring's state, not the eventfd
  count, is the source of truth for available records.
- The consumer drains payloads into a bounded in-process queue. A dedicated
  writer thread writes them to the required `--file-path` output, allowing ring
  draining to proceed while file output is in progress. By default, payloads
  are written as hexadecimal lines; `--binary-output` writes raw payload bytes
  instead. Payload data is not printed to the console.
- When queue occupancy reaches the 90% high watermark, the producer pauses.
  As the consumer drains the queue through the 40% low watermark, it signals a
  second eventfd. The producer waits for that notification with `epoll` and
  rechecks queue occupancy before resuming. This high/low watermark policy
  applies backpressure instead of allowing an unbounded queue to grow.
- The producer sends a fixed number of messages, marks the queue complete, and
  waits for the consumer to drain the shared-memory ring. The consumer flushes
  queued file output before exiting and reports both ring-drain and total
  throughput.
- `SIGINT` and `SIGTERM` stop production and let an attached consumer drain the
  ring and flush its output before the producer exits. If no consumer connects
  within three seconds, or an attached consumer does not finish within thirty
  seconds, the producer reports incomplete shutdown and exits rather than
  waiting indefinitely. After opening the queue, the consumer also gives the
  notification handoff three seconds to complete, so it does not retry forever
  if the producer exits during startup. The consumer handles signals during
  startup and handoff; during a transfer it drains the producer's queue before
  exiting with the corresponding signal status.

The producer creates both eventfds. For initial setup only, it passes them to
the consumer using `SCM_RIGHTS` over a local abstract Unix-domain socket. This
small control-plane handoff does not carry log records; the data plane remains
the shared-memory ring. A larger event-driven producer can register the
space-available eventfd alongside its other descriptors in the same `epoll`
loop. In this sample, the producer has no separate input or TLS work to service
while paused, so it waits for queue-space notifications or shutdown signals.

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
root. The consumer waits for the producer to create and initialize the queue.
Its output file is required:

```sh
./build/release/consumer --file-path payloads.hex
./build/release/producer
```

The producer sends 1,000 messages by default. Payload contents are random and
payload lengths are randomized from 1,024 through 2,024 bytes. The output uses
two hexadecimal characters per payload byte, so this default run writes about
3 MB to the output file. Use `--messages` to change the message count or
`--payload-bytes` to use a fixed payload length (1 through 2,024 bytes):

```sh
./build/release/producer --messages 100 --payload-bytes 2024
```

`--messages` must be positive. If `--payload-bytes` is omitted, each message
gets a random payload length from 1,024 through 2,024 bytes. The consumer
requires `--file-path` and truncates the named output file before writing lines
in `event-id:length: [hex-payload]` format. Add `--binary-output` to write the
raw payload bytes concatenated, with no event IDs, lengths, or record
boundaries. Use a fixed `--payload-bytes` value when record boundaries need to
be inferred from the binary output.

For a raw binary-output run with fixed-size messages:

```sh
./build/release/consumer --file-path payloads.bin --binary-output
./build/release/producer --messages 1000000 --payload-bytes 2024
```

CLI help is available at both endpoints:

```sh
./build/release/producer --help
./build/release/consumer --help
```

## Transfer summaries

The producer reports `total_messages_sent`, `total_payload_bytes_sent`,
`total_elapsed_time_seconds`, and `transfer_rate_bytes_per_second`. The
consumer reports the corresponding `total_messages_received` and
`total_payload_bytes_received` fields, plus the same total elapsed-time and
rate fields. Payload sizes and readable rates use decimal units (1 kB = 1,000
bytes); the raw rate is also printed in B/s. Payload byte counts do not include
record metadata or output formatting overhead. In default text mode, each
payload byte expands to two hexadecimal characters.

The producer's elapsed time starts immediately before it begins producing
messages and ends after the consumer reports that the shared-memory ring has
drained. It includes random payload generation, queue backpressure, and waiting
for the consumer to drain the ring; it does not wait for the consumer's file
writer to finish flushing.

The consumer reports `ring_drain_elapsed_seconds` and
`ring_drain_transfer_rate_bytes_per_second` from the start of its receive loop
until the shared-memory ring is empty. It also reports total elapsed time and
transfer rate; these include waiting for input and finishing/flushing the
hexadecimal file writer. These are end-to-end sample measurements, not isolated
shared-memory bandwidth benchmarks: payload generation, queueing, scheduling,
and file output affect the results. Binary output avoids hexadecimal
conversion and writes half as many payload-representation bytes, but does not
remove producer-side random generation or IPC overhead.

The debug binaries are `build/debug/consumer` and `build/debug/producer`; run
both endpoints from the same build type. Sanitizers add runtime overhead and
are intended for debugging, not performance measurements. Build both sets
with `make` or just one set with `make release` / `make debug`. Remove generated
binaries with `make clean`.

## Code map

- `spsc_ringbuffer.hpp` — fixed-capacity SPSC ring buffer and an alternate
  implementation retained for comparison.
- `shared_queue_storage.hpp` — in-place shared-memory layout, compatibility
  metadata, ring, and endpoint-completion flags.
- `shm_log_queue_mapping.hpp` / `shm_log_queue_mapping.cpp` — shared-memory
  creation, mapping, readiness, role-checked queue operations, and cleanup.
- `shm_queue_eventfd.hpp` / `shm_queue_eventfd.cpp` — eventfd notifications,
  eventfd descriptor handoff, and move-only file-descriptor ownership.
- `shm_Prod_QueueOps.cpp` — sample event producer and its `epoll` loop.
- `shm_Consum_QueueOps.cpp` — sample consumer, `epoll` loop, and file output.
- `log_queue_config.hpp` — `LogEvent`, queue capacity, watermarks, and IPC
  names.
- `shm_ringbuffer_tests.cpp` — standalone ring-buffer test/example source.

Queue operations use `EnqueueResult` (`enqueued` or `full`) and
`DequeueResult` (`dequeued` or `empty`), so full/empty are explicit normal
outcomes rather than generic failures. Interruptible startup and signal waits
also return named states. Eventfd operations return
`std::expected<void, std::error_code>`: `has_value()` means success, while
`error()` identifies the failed system operation.

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
named shared-memory and semaphore resources during normal cleanup. `SIGKILL`
cannot be caught, so it can leave those names behind; when a new producer
successfully binds the exclusive notification socket, it removes such stale
names before creating a fresh queue. If the consumer was also left running, it
must be restarted so it attaches to the new queue. The IPC setup, `eventfd`,
`signalfd`, and `epoll` implementation is Linux-specific.
