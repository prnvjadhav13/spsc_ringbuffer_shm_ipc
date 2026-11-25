"""Measure SPSC IPC throughput, process CPU use, and context switches on Linux."""

import os
import pathlib
import re
import subprocess
import tempfile
import threading
import time
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
PRODUCER = ROOT / "build" / "release" / "producer"
CONSUMER = ROOT / "build" / "release" / "consumer"
PAYLOAD_SIZES = (64, 256, 1024, 2024)
MESSAGE_COUNT = int(os.environ.get("SPSC_IPC_TEST_MESSAGES", "20000"))
PROC_TICKS_PER_SECOND = os.sysconf("SC_CLK_TCK")
SUMMARY_PATTERN = re.compile(r"^  ([a-z0-9_]+)=([^\n]+)$", re.MULTILINE)


def parse_summary(output):
    return dict(SUMMARY_PATTERN.findall(output))


def parse_bytes(summary_value):
    match = re.match(r"(\d+)", summary_value)
    if match is None:
        raise AssertionError(f"Unexpected byte total: {summary_value}")
    return int(match.group(1))


def read_process_cpu_ticks(pid):
    try:
        stat = pathlib.Path(f"/proc/{pid}/stat").read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    close_paren = stat.rfind(")")
    fields = stat[close_paren + 2 :].split()
    if len(fields) < 13:
        return None
    # The remaining fields start with stat field 3 (state); utime/stime are 14/15.
    return int(fields[11]) + int(fields[12])


class ProcessMonitor:
    """Sample /proc CPU time and per-thread context-switch counters."""

    def __init__(self, pids, interval_seconds=0.02):
        self.pids = tuple(pids)
        self.interval_seconds = interval_seconds
        self.cpu_count = os.cpu_count() or 1
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._last_cpu_ticks = {}
        self._last_thread_switches = {}
        self._cpu_seconds = 0.0
        self._voluntary_switches = 0
        self._involuntary_switches = 0
        self._peak_cpu_percent = 0.0
        self._sample_count = 0
        self._started_at = None
        self._finished_at = None

    def start(self):
        self._started_at = time.monotonic()
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()
        self._finished_at = time.monotonic()
        return self.metrics()

    def _sample(self, now, previous_time):
        delta_ticks = 0
        for pid in self.pids:
            cpu_ticks = read_process_cpu_ticks(pid)
            if cpu_ticks is not None:
                previous_ticks = self._last_cpu_ticks.get(pid)
                if previous_ticks is not None:
                    delta_ticks += max(0, cpu_ticks - previous_ticks)
                self._last_cpu_ticks[pid] = cpu_ticks

            task_directory = pathlib.Path(f"/proc/{pid}/task")
            try:
                tids = [int(path.name) for path in task_directory.iterdir()]
            except (FileNotFoundError, ProcessLookupError, PermissionError):
                continue
            for tid in tids:
                switches = read_thread_context_switches_for_tid(pid, tid)
                if switches is None:
                    continue
                key = (pid, tid)
                previous_switches = self._last_thread_switches.get(key)
                if previous_switches is not None:
                    self._voluntary_switches += max(0, switches[0] - previous_switches[0])
                    self._involuntary_switches += max(0, switches[1] - previous_switches[1])
                self._last_thread_switches[key] = switches

        elapsed = now - previous_time
        if elapsed > 0:
            delta_cpu_seconds = delta_ticks / PROC_TICKS_PER_SECOND
            self._cpu_seconds += delta_cpu_seconds
            cpu_percent = delta_cpu_seconds / elapsed / self.cpu_count * 100.0
            self._peak_cpu_percent = max(self._peak_cpu_percent, cpu_percent)
        self._sample_count += 1

    def _run(self):
        previous_time = time.monotonic()
        while not self._stop.wait(self.interval_seconds):
            now = time.monotonic()
            self._sample(now, previous_time)
            previous_time = now

    def metrics(self):
        wall_seconds = max(0.0, self._finished_at - self._started_at)
        average_cpu_percent = (
            self._cpu_seconds / wall_seconds / self.cpu_count * 100.0
            if wall_seconds > 0
            else 0.0
        )
        return {
            "wall_seconds": wall_seconds,
            "cpu_seconds": self._cpu_seconds,
            "average_cpu_percent_of_machine": average_cpu_percent,
            "peak_cpu_percent_of_machine": self._peak_cpu_percent,
            "voluntary_context_switches": self._voluntary_switches,
            "involuntary_context_switches": self._involuntary_switches,
            "samples": self._sample_count,
        }


def read_thread_context_switches_for_tid(pid, tid):
    status_path = pathlib.Path(f"/proc/{pid}/task/{tid}/status")
    voluntary = None
    involuntary = None
    try:
        for line in status_path.read_text().splitlines():
            if line.startswith("voluntary_ctxt_switches:"):
                voluntary = int(line.split()[1])
            elif line.startswith("nonvoluntary_ctxt_switches:"):
                involuntary = int(line.split()[1])
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    if voluntary is None or involuntary is None:
        return None
    return voluntary, involuntary


def run_transfer(messages, payload_bytes, consumer_first=True, monitor=True):
    with tempfile.TemporaryDirectory(prefix="spsc-ipc-test-") as working_directory:
        consumer = None
        producer = None
        process_monitor = None
        consumer_arguments = [
            str(CONSUMER),
            "--file-path",
            "/dev/null",
            "--binary-output",
        ]
        producer_arguments = [
            str(PRODUCER),
            "--messages",
            str(messages),
            "--payload-bytes",
            str(payload_bytes),
        ]
        try:
            if consumer_first:
                consumer = subprocess.Popen(
                    consumer_arguments,
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                producer = subprocess.Popen(
                    producer_arguments,
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            else:
                producer = subprocess.Popen(
                    producer_arguments,
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                time.sleep(0.1)
                consumer = subprocess.Popen(
                    consumer_arguments,
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

            if monitor:
                process_monitor = ProcessMonitor((producer.pid, consumer.pid))
                process_monitor.start()

            producer_stdout, producer_stderr = producer.communicate(timeout=180)
            consumer_stdout, consumer_stderr = consumer.communicate(timeout=180)
            metrics = process_monitor.stop() if process_monitor is not None else None
        finally:
            if process_monitor is not None and process_monitor._thread.is_alive():
                metrics = process_monitor.stop()
            for process in (consumer, producer):
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.communicate(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.communicate()

    return (
        producer.returncode,
        producer_stdout,
        producer_stderr,
        consumer.returncode,
        consumer_stdout,
        consumer_stderr,
        metrics,
    )


@unittest.skipUnless(pathlib.Path("/proc/self/stat").exists(), "Linux /proc is required")
class IpcThroughputTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not PRODUCER.is_file() or not CONSUMER.is_file():
            raise unittest.SkipTest("build release producer and consumer first with `make release`")

    def test_throughput_and_resource_profile_for_payload_sizes(self):
        for payload_bytes in PAYLOAD_SIZES:
            with self.subTest(payload_bytes=payload_bytes):
                (
                    producer_status,
                    producer_stdout,
                    producer_stderr,
                    consumer_status,
                    consumer_stdout,
                    consumer_stderr,
                    metrics,
                ) = run_transfer(MESSAGE_COUNT, payload_bytes)
                self.assertEqual(producer_status, 0, producer_stderr)
                self.assertEqual(consumer_status, 0, consumer_stderr)

                producer = parse_summary(producer_stdout)
                consumer = parse_summary(consumer_stdout)
                expected_bytes = MESSAGE_COUNT * payload_bytes
                self.assertEqual(int(producer["total_messages_sent"]), MESSAGE_COUNT)
                self.assertEqual(int(consumer["total_messages_received"]), MESSAGE_COUNT)
                self.assertEqual(parse_bytes(producer["total_payload_bytes_sent"]), expected_bytes)
                self.assertEqual(
                    parse_bytes(consumer["total_payload_bytes_received"]), expected_bytes
                )

                producer_mbps = float(
                    re.match(r"([\d.]+)", producer["transfer_rate_bytes_per_second"]).group(1)
                ) / 1_000_000
                consumer_mbps = float(
                    re.match(
                        r"([\d.]+)",
                        consumer["ring_drain_transfer_rate_bytes_per_second"],
                    ).group(1)
                ) / 1_000_000
                self.assertGreater(producer_mbps, 0)
                self.assertGreater(consumer_mbps, 0)
                self.assertIsNotNone(metrics)

                print(
                    "IPC profile: "
                    f"payload={payload_bytes} B, messages={MESSAGE_COUNT}, "
                    f"producer={producer_mbps:.2f} MB/s, "
                    f"consumer_ring={consumer_mbps:.2f} MB/s, "
                    f"CPU avg/peak={metrics['average_cpu_percent_of_machine']:.1f}%/"
                    f"{metrics['peak_cpu_percent_of_machine']:.1f}% of machine, "
                    f"context switches voluntary/involuntary="
                    f"{metrics['voluntary_context_switches']}/"
                    f"{metrics['involuntary_context_switches']}, "
                    f"samples={metrics['samples']}"
                )

    def test_backpressure_with_producer_started_before_consumer(self):
        messages = 5000
        payload_bytes = 128
        (
            producer_status,
            producer_stdout,
            producer_stderr,
            consumer_status,
            consumer_stdout,
            consumer_stderr,
            metrics,
        ) = run_transfer(messages, payload_bytes, consumer_first=False)
        self.assertEqual(producer_status, 0, producer_stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(int(parse_summary(producer_stdout)["total_messages_sent"]), messages)
        self.assertEqual(
            int(parse_summary(consumer_stdout)["total_messages_received"]), messages
        )
        self.assertIsNotNone(metrics)
        print(
            "IPC backpressure profile: "
            f"payload={payload_bytes} B, messages={messages}, "
            f"CPU avg/peak={metrics['average_cpu_percent_of_machine']:.1f}%/"
            f"{metrics['peak_cpu_percent_of_machine']:.1f}% of machine, "
            f"context switches voluntary/involuntary="
            f"{metrics['voluntary_context_switches']}/"
            f"{metrics['involuntary_context_switches']}"
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
