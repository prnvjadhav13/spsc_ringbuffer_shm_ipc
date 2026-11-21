import math
import pathlib
import re
import signal
import subprocess
import tempfile
import time
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
PRODUCER = ROOT / "build" / "debug" / "producer"
CONSUMER = ROOT / "build" / "debug" / "consumer"
MIN_PAYLOAD_BYTES = 1024
MAX_PAYLOAD_BYTES = 1024 * 1024


def run_cli(binary, *arguments):
    return subprocess.run(
        [str(binary), *arguments],
        capture_output=True,
        text=True,
        timeout=5,
    )


def summary_values(output):
    return dict(re.findall(r"^  ([a-z0-9_]+)=([^\n]+)$", output, re.MULTILINE))


def parse_byte_rate(value):
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?) \[([0-9]+(?:\.[0-9]+)? "
        r"(?:B/s|kB/s|MB/s|GB/s|TB/s|PB/s|EB/s))\]",
        value,
    )
    if match is None:
        raise AssertionError(f"Unexpected byte-rate format: {value}")
    unit_multipliers = {
        "B/s": 1,
        "kB/s": 1_000,
        "MB/s": 1_000_000,
        "GB/s": 1_000_000_000,
        "TB/s": 1_000_000_000_000,
        "PB/s": 1_000_000_000_000_000,
        "EB/s": 1_000_000_000_000_000_000,
    }
    raw_bytes_per_second = float(match.group(1))
    formatted_rate = match.group(2)
    formatted_match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?) (B/s|kB/s|MB/s|GB/s|TB/s|PB/s|EB/s)",
        formatted_rate,
    )
    if formatted_match is None:
        raise AssertionError(f"Unexpected formatted byte rate: {formatted_rate}")
    unit_multiplier = unit_multipliers[formatted_match.group(2)]
    expected_bytes_per_second = float(formatted_match.group(1)) * unit_multiplier
    if not math.isclose(
        raw_bytes_per_second,
        expected_bytes_per_second,
        rel_tol=0.0,
        abs_tol=0.050001 * unit_multiplier,
    ):
        raise AssertionError(f"Inconsistent byte-rate formats: {value}")
    return raw_bytes_per_second


def parse_payload_byte_total(value):
    match = re.fullmatch(
        r"(\d+) \[[0-9]+(?:\.[0-9]+)? (?:B|kB|MB|GB|TB|PB|EB)\]", value
    )
    if match is None:
        raise AssertionError(f"Unexpected payload-byte format: {value}")
    return int(match.group(1))


def run_transfer(messages, payload_bytes=None, initial_output=None, binary_output=False):
    with tempfile.TemporaryDirectory() as working_directory:
        output_path = pathlib.Path(working_directory) / (
            "payloads.bin" if binary_output else "payloads.hex"
        )
        if initial_output is not None:
            output_path.write_text(initial_output)
        consumer_arguments = [str(CONSUMER), "--file-path", str(output_path)]
        if binary_output:
            consumer_arguments.append("--binary-output")
        consumer = subprocess.Popen(
            consumer_arguments,
            cwd=working_directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            producer_arguments = [str(PRODUCER), "--messages", str(messages)]
            if payload_bytes is not None:
                producer_arguments.extend(["--payload-bytes", str(payload_bytes)])
            producer = subprocess.run(
                producer_arguments,
                cwd=working_directory,
                capture_output=True,
                text=True,
                timeout=30,
            )
            consumer_stdout, consumer_stderr = consumer.communicate(timeout=30)
        finally:
            if consumer.poll() is None:
                consumer.terminate()
                try:
                    consumer.communicate(timeout=3)
                except subprocess.TimeoutExpired:
                    consumer.kill()
                    consumer.communicate()

        payload_output = (
            output_path.read_bytes()
            if binary_output
            else output_path.read_text().splitlines()
        )
    return producer, consumer.returncode, consumer_stdout, consumer_stderr, payload_output


class TransferCliTests(unittest.TestCase):
    def test_help_documents_finite_transfer_and_required_file(self):
        producer_help = run_cli(PRODUCER, "--help")
        self.assertEqual(producer_help.returncode, 0, producer_help.stderr)
        self.assertIn("--messages", producer_help.stdout)
        self.assertIn("--payload-bytes", producer_help.stdout)
        self.assertNotIn("--benchmark", producer_help.stdout)
        self.assertNotIn("--warmup", producer_help.stdout)

        consumer_help = run_cli(CONSUMER, "--help")
        self.assertEqual(consumer_help.returncode, 0, consumer_help.stderr)
        self.assertIn("--file-path output-file", consumer_help.stdout)
        self.assertIn("--binary-output", consumer_help.stdout)

    def test_invalid_cli_options_fail_before_startup(self):
        for arguments, expected_error in (
            (("--benchmark",), "Unknown argument: --benchmark"),
            (("--messages", "0"), "--messages must be positive"),
            (("--payload-bytes", "0"), "Invalid payload size"),
            (("--payload-bytes", str(MAX_PAYLOAD_BYTES + 1)), "must not exceed"),
        ):
            with self.subTest(arguments=arguments):
                result = run_cli(PRODUCER, *arguments)
                self.assertEqual(result.returncode, 2)
                self.assertIn(expected_error, result.stderr)

        missing_path = run_cli(CONSUMER)
        self.assertEqual(missing_path.returncode, 2)
        self.assertIn("Missing required option: --file-path", missing_path.stderr)

        duplicate_path = run_cli(
            CONSUMER, "--file-path", "one.hex", "--file-path", "two.hex"
        )
        self.assertEqual(duplicate_path.returncode, 2)
        self.assertIn("specified more than once: --file-path", duplicate_path.stderr)

        positional_path = run_cli(CONSUMER, "payloads.hex")
        self.assertEqual(positional_path.returncode, 2)
        self.assertIn("Unknown argument: payloads.hex", positional_path.stderr)

    def test_producer_exits_when_signaled_while_waiting_for_consumer(self):
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=stop_signal):
                producer = subprocess.Popen(
                    [str(PRODUCER), "--messages", "1", "--payload-bytes", "64"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    time.sleep(0.1)
                    self.assertIsNone(producer.poll(), "Producer exited before receiving signal")
                    producer.send_signal(stop_signal)
                    stdout, stderr = producer.communicate(timeout=5)
                    self.assertEqual(producer.returncode, 128 + stop_signal, stderr)
                    self.assertIn("total_messages_sent=1", stdout)
                    self.assertIn("timed out waiting for the consumer to connect", stderr)
                finally:
                    if producer.poll() is None:
                        producer.kill()
                        producer.communicate()

    def test_producer_signal_waits_for_consumer_to_flush_output(self):
        with tempfile.TemporaryDirectory() as working_directory:
            output_path = pathlib.Path(working_directory) / "payloads.hex"
            producer = subprocess.Popen(
                [
                    str(PRODUCER),
                    "--messages",
                    "100000",
                    "--payload-bytes",
                    "1024",
                ],
                cwd=working_directory,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            consumer = None
            try:
                consumer = subprocess.Popen(
                    [str(CONSUMER), "--file-path", str(output_path)],
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                deadline = time.monotonic() + 5
                while not output_path.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(output_path.exists(), "Consumer did not start")
                time.sleep(0.1)

                producer.send_signal(signal.SIGTERM)
                producer_stdout, producer_stderr = producer.communicate(timeout=15)
                consumer_stdout, consumer_stderr = consumer.communicate(timeout=15)

                self.assertEqual(producer.returncode, 128 + signal.SIGTERM, producer_stderr)
                self.assertEqual(consumer.returncode, 0, consumer_stderr)
                sent = int(
                    re.search(r"^  total_messages_sent=(\d+)$", producer_stdout, re.MULTILINE)
                    .group(1)
                )
                self.assertEqual(len(output_path.read_text().splitlines()), sent)
            finally:
                if consumer is not None and consumer.poll() is None:
                    consumer.kill()
                    consumer.communicate()
                if producer.poll() is None:
                    producer.kill()
                    producer.communicate()

    def test_consumer_exits_when_signaled_during_notification_handoff(self):
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=stop_signal):
                producer = subprocess.Popen(
                    [str(PRODUCER), "--messages", "1", "--payload-bytes", "64"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                consumer = None
                try:
                    time.sleep(0.1)
                    self.assertIsNone(producer.poll(), "Producer exited before consumer startup")
                    producer.send_signal(signal.SIGSTOP)
                    with tempfile.TemporaryDirectory() as working_directory:
                        output_path = pathlib.Path(working_directory) / "payloads.hex"
                        consumer = subprocess.Popen(
                            [str(CONSUMER), "--file-path", str(output_path)],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                        )
                        deadline = time.monotonic() + 5
                        while not output_path.exists() and time.monotonic() < deadline:
                            time.sleep(0.01)
                        self.assertTrue(output_path.exists(), "Consumer did not start")
                        time.sleep(0.1)
                        consumer.send_signal(stop_signal)
                        _, stderr = consumer.communicate(timeout=5)
                        self.assertEqual(consumer.returncode, 128 + stop_signal, stderr)
                finally:
                    if consumer is not None and consumer.poll() is None:
                        consumer.kill()
                        consumer.communicate()
                    if producer.poll() is None:
                        producer.send_signal(signal.SIGCONT)
                        producer.send_signal(signal.SIGTERM)
                        try:
                            producer.communicate(timeout=5)
                        except subprocess.TimeoutExpired:
                            producer.kill()
                            producer.communicate()

    def test_consumer_exits_when_signaled_while_producer_is_active(self):
        for stop_signal in (signal.SIGINT, signal.SIGTERM):
            with (
                self.subTest(signal=stop_signal),
                tempfile.TemporaryDirectory() as working_directory,
            ):
                output_path = pathlib.Path(working_directory) / "payloads.hex"
                producer = subprocess.Popen(
                    [str(PRODUCER), "--messages", "1000000000", "--payload-bytes", "64"],
                    cwd=working_directory,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                consumer = None
                try:
                    consumer = subprocess.Popen(
                        [str(CONSUMER), "--file-path", str(output_path)],
                        cwd=working_directory,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    deadline = time.monotonic() + 5
                    while not output_path.exists() and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(output_path.exists(), "Consumer did not start")
                    time.sleep(0.1)
                    self.assertIsNone(producer.poll(), "Producer exited before consumer signal")

                    consumer.send_signal(stop_signal)
                    consumer_stdout, consumer_stderr = consumer.communicate(timeout=5)

                    self.assertEqual(consumer.returncode, 128 + stop_signal, consumer_stderr)
                    self.assertIn("Consumer transfer summary:", consumer_stdout)
                finally:
                    if consumer is not None and consumer.poll() is None:
                        consumer.kill()
                        consumer.communicate()
                    if producer.poll() is None:
                        producer.kill()
                        producer.communicate()

    def test_producer_recovers_resources_left_by_forced_termination(self):
        first_producer = subprocess.Popen(
            [str(PRODUCER), "--messages", "1", "--payload-bytes", "64"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        restarted_producer = None
        consumer = None
        shared_memory = pathlib.Path("/dev/shm/shm_log_ring_buffer")
        readiness_semaphore = pathlib.Path("/dev/shm/sem.sem_log_ring_buffer_ready")
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if shared_memory.exists() and readiness_semaphore.exists():
                    break
                if first_producer.poll() is not None:
                    stdout, stderr = first_producer.communicate()
                    self.fail(f"Initial producer exited unexpectedly: {stdout}\n{stderr}")
                time.sleep(0.01)
            else:
                self.fail("Producer did not create its shared-memory resources")

            first_producer.kill()
            first_producer.communicate(timeout=5)

            restarted_producer = subprocess.Popen(
                [str(PRODUCER), "--messages", "1", "--payload-bytes", "64"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                time.sleep(0.2)
                if restarted_producer.poll() is not None:
                    stdout, stderr = restarted_producer.communicate()
                    self.fail(f"Restarted producer exited early: {stdout}\n{stderr}")
                with tempfile.TemporaryDirectory() as working_directory:
                    output_path = pathlib.Path(working_directory) / "payloads.hex"
                    consumer = subprocess.Popen(
                        [str(CONSUMER), "--file-path", str(output_path)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    producer_stdout, producer_stderr = restarted_producer.communicate(
                        timeout=30
                    )
                    consumer_stdout, consumer_stderr = consumer.communicate(timeout=30)
                    self.assertEqual(restarted_producer.returncode, 0, producer_stderr)
                    self.assertEqual(consumer.returncode, 0, consumer_stderr)
                    self.assertIn("total_messages_sent=1", producer_stdout)
                    self.assertIn("total_messages_received=1", consumer_stdout)
            finally:
                if consumer is not None and consumer.poll() is None:
                    consumer.kill()
                    consumer.communicate()
        finally:
            if first_producer.poll() is None:
                first_producer.kill()
                first_producer.communicate()
            if restarted_producer is not None and restarted_producer.poll() is None:
                restarted_producer.kill()
                restarted_producer.communicate()

    def test_fixed_payloads_are_written_only_to_file_and_transfer_stats_match(self):
        messages = 5
        payload_bytes = 64
        producer, consumer_status, consumer_stdout, consumer_stderr, payload_lines = (
            run_transfer(messages, payload_bytes)
        )

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(len(payload_lines), messages)
        self.assertTrue(
            all(
                re.fullmatch(rf"{event_id}:{payload_bytes}: \[[0-9a-f]{{128}}\]", line)
                for event_id, line in enumerate(payload_lines, start=1)
            )
        )
        self.assertNotIn("Payload", producer.stdout)
        self.assertNotIn("Payload", consumer_stdout)

        producer_stats = summary_values(producer.stdout)
        consumer_stats = summary_values(consumer_stdout)
        for stats, count_key, payload_key in (
            (producer_stats, "total_messages_sent", "total_payload_bytes_sent"),
            (consumer_stats, "total_messages_received", "total_payload_bytes_received"),
        ):
            self.assertEqual(int(stats[count_key]), messages)
            self.assertEqual(
                parse_payload_byte_total(stats[payload_key]),
                messages * payload_bytes,
            )
            elapsed = float(stats["total_elapsed_time_seconds"])
            self.assertGreater(elapsed, 0)
            self.assertTrue(
                math.isclose(
                    parse_byte_rate(stats["transfer_rate_bytes_per_second"]),
                    messages * payload_bytes / elapsed,
                    rel_tol=0.01,
                )
            )
            if count_key == "total_messages_received":
                ring_drain_elapsed = float(stats["ring_drain_elapsed_seconds"])
                self.assertGreater(ring_drain_elapsed, 0)
                self.assertTrue(
                    math.isclose(
                        parse_byte_rate(
                            stats["ring_drain_transfer_rate_bytes_per_second"]
                        ),
                        messages * payload_bytes / ring_drain_elapsed,
                        rel_tol=0.01,
                    )
                )

    def test_binary_output_contains_only_concatenated_payload_bytes(self):
        messages = 5
        payload_bytes = 64
        producer, consumer_status, consumer_stdout, consumer_stderr, payload_output = (
            run_transfer(messages, payload_bytes, binary_output=True)
        )

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(len(payload_output), messages * payload_bytes)
        self.assertEqual(
            parse_payload_byte_total(
                summary_values(consumer_stdout)["total_payload_bytes_received"]
            ),
            len(payload_output),
        )

    def test_payload_byte_summary_includes_decimal_size(self):
        producer, consumer_status, consumer_stdout, consumer_stderr, _ = run_transfer(
            1, MAX_PAYLOAD_BYTES
        )

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(
            summary_values(producer.stdout)["total_payload_bytes_sent"],
            f"{MAX_PAYLOAD_BYTES} [1.05 MB]",
        )
        self.assertEqual(
            summary_values(consumer_stdout)["total_payload_bytes_received"],
            f"{MAX_PAYLOAD_BYTES} [1.05 MB]",
        )

    def test_writer_failure_is_reported_after_shared_queue_is_drained(self):
        with tempfile.TemporaryDirectory() as working_directory:
            consumer = subprocess.Popen(
                [str(CONSUMER), "--file-path", "/dev/full"],
                cwd=working_directory,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                producer = subprocess.run(
                    [str(PRODUCER), "--messages", "2", "--payload-bytes", "64"],
                    cwd=working_directory,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                _, consumer_stderr = consumer.communicate(timeout=30)
            finally:
                if consumer.poll() is None:
                    consumer.terminate()
                    consumer.communicate()

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer.returncode, 1, consumer_stderr)
        self.assertIn("Failed to flush payload output file: /dev/full", consumer_stderr)

    def test_existing_output_file_is_truncated_before_transfer(self):
        messages = 2
        producer, consumer_status, _, consumer_stderr, payload_lines = run_transfer(
            messages, 64, initial_output="stale output\n"
        )

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(len(payload_lines), messages)
        self.assertTrue(
            all(
                re.fullmatch(rf"{event_id}:64: \[[0-9a-f]{{128}}\]", line)
                for event_id, line in enumerate(payload_lines, start=1)
            )
        )

    def test_random_payload_sizes_are_preserved_in_hex_file(self):
        messages = 8
        producer, consumer_status, consumer_stdout, consumer_stderr, payload_lines = (
            run_transfer(messages)
        )

        self.assertEqual(producer.returncode, 0, producer.stderr)
        self.assertEqual(consumer_status, 0, consumer_stderr)
        self.assertEqual(len(payload_lines), messages)
        records = [
            re.fullmatch(r"(\d+):(\d+): \[([0-9a-f]+)\]", line)
            for line in payload_lines
        ]
        self.assertTrue(all(records))
        self.assertEqual([int(record.group(1)) for record in records], list(range(1, messages + 1)))
        payload_sizes = [int(record.group(2)) for record in records]
        for record, payload_size in zip(records, payload_sizes):
            self.assertEqual(len(record.group(3)), payload_size * 2)
            self.assertGreaterEqual(payload_size, MIN_PAYLOAD_BYTES)
            self.assertLessEqual(payload_size, MAX_PAYLOAD_BYTES)

        producer_stats = summary_values(producer.stdout)
        consumer_stats = summary_values(consumer_stdout)
        self.assertEqual(
            parse_payload_byte_total(producer_stats["total_payload_bytes_sent"]),
            sum(payload_sizes),
        )
        self.assertEqual(
            parse_payload_byte_total(consumer_stats["total_payload_bytes_received"]),
            sum(payload_sizes),
        )


if __name__ == "__main__":
    unittest.main()
