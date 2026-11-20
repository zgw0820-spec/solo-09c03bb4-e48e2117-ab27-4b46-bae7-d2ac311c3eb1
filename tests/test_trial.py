"""End-to-end tests for the REF-001 trial runs (flooding output after the report)."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.db import database_fingerprint
from app.dispatcher import Dispatcher

from .conftest import wait_all_done, wait_for

MIB = 1024 * 1024


def test_trial_exit_passes_and_keeps_buffer_capped(dispatcher, settings, baseline):
    """Trial 'exit': commit, print the passing report, flood 8 MiB per stream, exit 0."""
    dispatcher.submit("REF-001", trial="exit")
    state = wait_all_done(dispatcher, timeout=60)
    task = state["tasks"][0]

    assert task["trial"] == "exit"
    result = task["result"]
    assert result["passed"] is True, result
    assert result["exit_code"] == 0
    assert result["timed_out"] is False

    output = result["output"]
    # Both streams received at least 8 MiB of ordinary logs after the report...
    assert output["stdout_bytes"] >= 8 * MIB
    assert output["stderr_bytes"] >= 8 * MIB
    # ...yet the retained buffer never exceeded the 256 KiB budget...
    assert output["buffer_peak_bytes"] <= 256 * 1024
    assert output["buffer_bytes"] <= 256 * 1024
    # ...old log bytes were dropped (and accounted)...
    assert output["stdout_dropped_bytes"] > 0
    assert output["stderr_dropped_bytes"] > 0
    # ...and the report survived the flood.
    assert 0 < output["report_bytes"] <= 64 * 1024

    # The refund was committed and then restored per the usual rules.
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_trial_timeout_is_killed_fails_and_restores(settings, baseline):
    """Trial 'timeout': floods until the dispatcher kills the process group."""
    short = Settings(
        data_dir=settings.data_dir,
        db_path=settings.db_path,
        seed_snapshot_path=settings.seed_snapshot_path,
        max_audit_concurrency=2,
        child_timeout_seconds=3,
    )
    instance = Dispatcher(short)
    instance.start()
    try:
        instance.submit("REF-001", trial="timeout")

        def finished() -> dict | None:
            current = instance.state()
            busy = any(t["status"] in ("queued", "running", "restoring") for t in current["tasks"])
            return None if busy or not current["tasks"] else current

        state = wait_for(finished, timeout=60)
    finally:
        instance.stop()

    task = state["tasks"][0]
    result = task["result"]
    assert result["timed_out"] is True
    assert result["passed"] is False
    assert "超时" in result["message"]
    # The report was printed before the flood but a timeout is always a failure.
    assert "不予采信" in result["message"]
    assert result["exit_code"] not in (0, None)

    output = result["output"]
    assert output["stdout_bytes"] >= 8 * MIB
    assert output["stderr_bytes"] >= 8 * MIB
    assert output["buffer_peak_bytes"] <= 256 * 1024

    # The stderr tail is shown and marked as truncated.
    assert output["stderr_dropped_bytes"] > 0
    assert result["stderr_tail"].startswith("…[已截断：丢弃前 ")

    # The process group was killed and the database restored afterwards.
    assert task["restore"]["ok"] is True
    assert database_fingerprint(settings.db_path) == baseline


def test_trial_rejected_for_other_presets(dispatcher):
    with pytest.raises(ValueError, match="仅支持"):
        dispatcher.submit("AUD-001", trial="exit")
    with pytest.raises(ValueError, match="仅支持"):
        dispatcher.submit("REF-002", trial="timeout")


def test_trial_unknown_mode_rejected(dispatcher):
    with pytest.raises(ValueError, match="未知试跑模式"):
        dispatcher.submit("REF-001", trial="bogus")


def test_trial_exit_also_restores_between_copies(dispatcher, settings, baseline):
    """Two trial runs in a row: each commits and is restored before the next."""
    dispatcher.submit("REF-001", copies=2, trial="exit")
    state = wait_all_done(dispatcher, timeout=90)
    assert [t["result"]["passed"] for t in state["tasks"]] == [True, True]
    assert all(t["restore"]["ok"] for t in state["tasks"])
    assert database_fingerprint(settings.db_path) == baseline
