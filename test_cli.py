import math
import pathlib
import re
import subprocess
import tempfile
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


def run_transfer(messages, payload_bytes=None, initial_output=None):
    with tempfile.TemporaryDirectory() as working_directory:
        output_path = pathlib.Path(working_directory) / "payloads.hex"
        if initial_output is not None:
            output_path.write_text(initial_output)
        consumer = subprocess.Popen(
            [str(CONSUMER), "--file-path", str(output_path)],
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

        payload_lines = output_path.read_text().splitlines()
    return producer, consumer.returncode, consumer_stdout, consumer_stderr, payload_lines


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
