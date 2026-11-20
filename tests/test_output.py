"""Unit tests for the bounded per-case output collector."""

from __future__ import annotations

import json

import pytest

from app.output import DEFAULT_REPORT_CAP, DEFAULT_TOTAL_CAP, CaseOutputCollector

MIB = 1024 * 1024


def _report_line(case_id: str = "REF-001", passed: bool = True, pad: int = 0) -> bytes:
    payload = {
        "case_id": case_id,
        "passed": passed,
        "message": "通过" + "x" * pad,
        "details": {},
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _flood_lines(stream: str, count: int, line_bytes: int = 1024) -> bytes:
    sample = f"[flood] {stream} #000000 \n"
    pad = "y" * (line_bytes - len(sample))
    return b"".join(f"[flood] {stream} #{i:06d} {pad}\n".encode() for i in range(count))


def test_total_cap_never_exceeded_under_heavy_flood():
    collector = CaseOutputCollector("REF-001")
    chunk = _flood_lines("stdout", 64)  # ~64 KiB per feed, like the reader threads
    rounds = 32
    for _ in range(rounds):  # ~2 MiB on each stream
        collector.feed("stdout", chunk)
        collector.feed("stderr", chunk.replace(b"stdout", b"stderr"))
    collector.finalize()
    stats = collector.stats()
    assert stats["stdout_bytes"] == rounds * len(chunk)
    assert stats["stderr_bytes"] == rounds * len(chunk)
    assert stats["stdout_bytes"] >= 2 * MIB
    assert stats["buffer_bytes"] <= DEFAULT_TOTAL_CAP
    assert stats["buffer_peak_bytes"] <= DEFAULT_TOTAL_CAP
    assert stats["stdout_dropped_bytes"] > 0
    assert stats["stderr_dropped_bytes"] > 0
    # Nothing retained beyond the cap even transiently.
    assert collector.buffered <= DEFAULT_TOTAL_CAP


def test_report_survives_subsequent_flood():
    collector = CaseOutputCollector("REF-001")
    report = _report_line()
    collector.feed("stdout", report + b"\n")
    for _ in range(16):
        collector.feed("stdout", _flood_lines("stdout", 64))
        collector.feed("stderr", _flood_lines("stderr", 64))
    collector.finalize()
    assert collector.report_line == report
    stats = collector.stats()
    assert stats["report_bytes"] == len(report)
    assert stats["buffer_peak_bytes"] <= DEFAULT_TOTAL_CAP


def test_last_qualifying_report_wins():
    collector = CaseOutputCollector("REF-001")
    first = _report_line(passed=True)
    second = _report_line(passed=False)
    collector.feed("stdout", first + b"\n")
    collector.feed("stdout", _flood_lines("stdout", 8))
    collector.feed("stdout", second + b"\n")
    collector.feed("stdout", _flood_lines("stdout", 8))
    collector.finalize()
    assert collector.report_line == second


def test_oversized_json_line_is_not_a_report():
    collector = CaseOutputCollector("REF-001")
    real = _report_line()
    collector.feed("stdout", real + b"\n")
    # A >64 KiB single-line JSON object with a matching case_id must not qualify.
    fake = _report_line(passed=False, pad=DEFAULT_REPORT_CAP)
    assert len(fake) > DEFAULT_REPORT_CAP
    collector.feed("stdout", fake + b"\n")
    collector.finalize()
    assert collector.report_line == real
    assert collector.buffered <= DEFAULT_TOTAL_CAP


def test_report_line_size_boundary():
    # Exactly report_cap bytes qualifies; one byte more does not.
    exact = _report_line(pad=10)
    exact = exact[:-1] + b" " * (DEFAULT_REPORT_CAP - len(exact)) + b"}"
    assert len(exact) == DEFAULT_REPORT_CAP
    collector = CaseOutputCollector("REF-001")
    collector.feed("stdout", exact + b"\n")
    collector.finalize()
    assert collector.report_line == exact

    over = exact + b" "
    collector = CaseOutputCollector("REF-001")
    collector.feed("stdout", over + b"\n")
    collector.finalize()
    assert collector.report_line is None


def test_report_without_trailing_newline_qualifies_at_eof():
    collector = CaseOutputCollector("REF-001")
    report = _report_line()
    collector.feed("stdout", report)  # no newline; EOF follows
    collector.finalize()
    assert collector.report_line == report


def test_wrong_case_id_and_non_json_do_not_qualify():
    collector = CaseOutputCollector("REF-001")
    collector.feed("stdout", _report_line(case_id="AUD-001") + b"\n")
    collector.feed("stdout", b'{"passed": true}\n')
    collector.feed("stdout", b"not json at all\n")
    collector.feed("stdout", b'["REF-001"]\n')
    collector.finalize()
    assert collector.report_line is None


def test_stderr_tail_marks_truncation():
    collector = CaseOutputCollector("REF-001")
    collector.feed("stderr", _flood_lines("stderr", 512))  # 512 KiB on stderr
    collector.finalize()
    stats = collector.stats()
    assert stats["stderr_dropped_bytes"] > 0
    tail = collector.tail_text("stderr", 4096)
    assert tail.startswith("…[已截断：丢弃前 ")
    assert "字节]…" in tail.split("\n", 1)[0]
    # An untruncated stream carries no marker.
    collector2 = CaseOutputCollector("REF-001")
    collector2.feed("stderr", b"short error\n")
    collector2.finalize()
    assert collector2.tail_text("stderr", 4096) == "short error\n"


def test_report_slot_counts_towards_total_budget():
    collector = CaseOutputCollector("REF-001")
    # Fill both tails to (near) the cap first.
    collector.feed("stdout", _flood_lines("stdout", 200))
    collector.feed("stderr", _flood_lines("stderr", 200))
    big_report = _report_line(pad=60 * 1024)
    assert len(big_report) <= DEFAULT_REPORT_CAP
    collector.feed("stdout", big_report + b"\n")
    collector.finalize()
    assert collector.report_line == big_report
    assert collector.buffered <= DEFAULT_TOTAL_CAP
    assert collector.stats()["buffer_peak_bytes"] <= DEFAULT_TOTAL_CAP


def test_chunk_boundaries_split_report_line():
    collector = CaseOutputCollector("REF-001")
    report = _report_line()
    data = report + b"\n"
    for i in range(0, len(data), 7):  # awkward 7-byte slices
        collector.feed("stdout", data[i : i + 7])
    collector.finalize()
    assert collector.report_line == report


def test_custom_small_caps():
    collector = CaseOutputCollector("REF-001", total_cap=4096, report_cap=1024)
    report = _report_line()
    collector.feed("stdout", report + b"\n")
    collector.feed("stdout", _flood_lines("stdout", 16))
    collector.feed("stderr", _flood_lines("stderr", 16))
    collector.finalize()
    assert collector.report_line == report
    assert collector.buffered <= 4096
    assert collector.stats()["buffer_peak_bytes"] <= 4096


def test_invalid_caps_rejected():
    with pytest.raises(ValueError):
        CaseOutputCollector("REF-001", total_cap=1024, report_cap=1024)
    with pytest.raises(ValueError):
        CaseOutputCollector("REF-001", total_cap=0, report_cap=1)
