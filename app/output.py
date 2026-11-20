"""Bounded in-memory capture of child-process output.

The dispatcher reads stdout and stderr continuously while a case runs, but it
never writes case logs to disk and never lets the retained output grow without
limit.  Each running case gets one :class:`CaseOutputCollector` whose total
retained bytes -- saved report line, partial-line assembly buffer and both
stream tails -- never exceed ``total_cap`` (default 256 KiB).

The single-line JSON report a case prints on stdout is protected: it lives in
its own slot, so later ordinary log output may evict older bytes from the
stream tails but can never push out the saved report.  Only lines of at most
``report_cap`` bytes (default 64 KiB) that parse as a JSON object with a
matching ``case_id`` qualify; the last qualifying line wins.
"""

from __future__ import annotations

import json
import threading

DEFAULT_TOTAL_CAP = 256 * 1024
DEFAULT_REPORT_CAP = 64 * 1024

TRUNCATION_NOTICE = "…[已截断：丢弃前 {dropped} 字节]…\n"


class CaseOutputCollector:
    """Thread-safe, bounded capture for one case's stdout/stderr.

    Invariant: ``len(report) + len(pending) + len(stdout_tail) +
    len(stderr_tail) <= total_cap`` holds at all times.  Bytes evicted from
    the front of a stream tail are accounted in ``dropped`` so the UI can
    mark the displayed tail as truncated.
    """

    def __init__(
        self,
        case_id: str,
        total_cap: int = DEFAULT_TOTAL_CAP,
        report_cap: int = DEFAULT_REPORT_CAP,
    ) -> None:
        if total_cap <= 0 or report_cap <= 0:
            raise ValueError("buffer caps must be positive")
        # The report slot and the line-assembly buffer are both bounded by
        # report_cap; requiring 2 * report_cap <= total_cap guarantees they
        # always fit within the budget no matter how full the tails are.
        if report_cap * 2 > total_cap:
            raise ValueError("report_cap must not exceed half of total_cap")
        self.case_id = case_id
        self.total_cap = total_cap
        self.report_cap = report_cap
        self._lock = threading.Lock()
        self._report: bytes | None = None
        self._pending = bytearray()  # stdout partial line being assembled
        self._pending_oversized = False  # current stdout line can never qualify
        self._tails: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
        self._received = {"stdout": 0, "stderr": 0}
        self._dropped = {"stdout": 0, "stderr": 0}
        self._peak = 0

    # -- intake (called from the reader threads) -----------------------------

    def feed(self, stream: str, data: bytes) -> None:
        """Consume one chunk read from ``stream`` (``"stdout"``/``"stderr"``)."""
        if not data:
            return
        with self._lock:
            self._received[stream] += len(data)
            if stream == "stdout":
                self._feed_stdout_locked(data)
            else:
                self._append_tail_locked("stderr", data)
            self._peak = max(self._peak, self._buffered_locked())

    def finalize(self) -> None:
        """Flush end-of-file state: a trailing line without ``\\n`` still counts."""
        with self._lock:
            if self._pending_oversized:
                self._pending_oversized = False
            elif self._pending:
                line = bytes(self._pending)
                self._pending.clear()
                stripped = line.strip()
                if stripped and self._qualifies(stripped):
                    self._set_report_locked(stripped)
                else:
                    self._append_tail_locked("stdout", line)
            self._peak = max(self._peak, self._buffered_locked())

    # -- results ---------------------------------------------------------------

    @property
    def report_line(self) -> bytes | None:
        """The last qualifying stdout report line (≤ report_cap), if any."""
        with self._lock:
            return self._report

    @property
    def buffered(self) -> int:
        """Currently retained bytes across all regions (≤ total_cap)."""
        with self._lock:
            return self._buffered_locked()

    def tail_text(self, stream: str, limit: int) -> str:
        """Decoded tail of a stream, prefixed with a truncation notice if the
        front of the stream was dropped to stay within the budget."""
        with self._lock:
            tail = bytes(self._tails[stream][-limit:]) if limit else bytes(self._tails[stream])
            dropped = self._dropped[stream]
        text = tail.decode("utf-8", errors="replace")
        if dropped:
            text = TRUNCATION_NOTICE.format(dropped=dropped) + text
        return text

    def stats(self) -> dict:
        with self._lock:
            return {
                "stdout_bytes": self._received["stdout"],
                "stderr_bytes": self._received["stderr"],
                "stdout_dropped_bytes": self._dropped["stdout"],
                "stderr_dropped_bytes": self._dropped["stderr"],
                "report_bytes": len(self._report) if self._report is not None else None,
                "buffer_bytes": self._buffered_locked(),
                "buffer_peak_bytes": self._peak,
                "buffer_cap_bytes": self.total_cap,
            }

    # -- internals (lock held by callers) ---------------------------------------

    def _buffered_locked(self) -> int:
        return (
            len(self._report or b"")
            + len(self._pending)
            + len(self._tails["stdout"])
            + len(self._tails["stderr"])
        )

    def _evict_locked(self, incoming: int, prefer_keep: str | None) -> None:
        """Drop bytes from the front of the stream tails so ``incoming`` fits."""
        overflow = self._buffered_locked() + incoming - self.total_cap
        if overflow <= 0:
            return
        order = [s for s in ("stdout", "stderr") if s != prefer_keep]
        if prefer_keep is not None:
            order.append(prefer_keep)
        for stream in order:
            if overflow <= 0:
                break
            tail = self._tails[stream]
            cut = min(len(tail), overflow)
            del tail[:cut]
            self._dropped[stream] += cut
            overflow -= cut

    def _append_tail_locked(self, stream: str, data: bytes) -> None:
        if not data:
            return
        if len(data) > self.total_cap:
            # A single piece larger than the whole budget: keep only its end.
            excess = len(data) - self.total_cap
            self._dropped[stream] += excess
            data = data[-self.total_cap :]
        other = "stderr" if stream == "stdout" else "stdout"
        self._evict_locked(len(data), prefer_keep=other)
        overflow = self._buffered_locked() + len(data) - self.total_cap
        if overflow > 0:
            # Report + pending leave less room than len(data): trim the front.
            self._dropped[stream] += overflow
            data = data[overflow:]
        self._tails[stream] += data

    def _feed_stdout_locked(self, data: bytes) -> None:
        pos = 0
        end_of_data = len(data)
        while pos < end_of_data:
            newline = data.find(b"\n", pos)
            end = end_of_data if newline < 0 else newline
            piece = data[pos:end]
            if piece:
                if self._pending_oversized:
                    self._append_tail_locked("stdout", piece)
                else:
                    self._append_pending_locked(piece)
            if newline < 0:
                break
            self._end_stdout_line_locked()
            pos = newline + 1

    def _append_pending_locked(self, piece: bytes) -> None:
        if len(self._pending) + len(piece) > self.report_cap:
            # This line exceeds the report size limit and can never qualify:
            # spill what we held to the stdout tail and treat the rest of the
            # line as ordinary log output.
            self._append_tail_locked("stdout", bytes(self._pending))
            self._pending.clear()
            self._pending_oversized = True
            self._append_tail_locked("stdout", piece)
            return
        self._evict_locked(len(piece), prefer_keep="stderr")
        self._pending += piece

    def _end_stdout_line_locked(self) -> None:
        if self._pending_oversized:
            self._pending_oversized = False
            self._append_tail_locked("stdout", b"\n")
            return
        line = bytes(self._pending)
        self._pending.clear()
        stripped = line.strip()
        if stripped and self._qualifies(stripped):
            self._set_report_locked(stripped)
        else:
            self._append_tail_locked("stdout", line + b"\n")

    def _qualifies(self, line: bytes) -> bool:
        """True for a well-formed report line for this case (size already bounded)."""
        if len(line) > self.report_cap or not line.startswith(b"{"):
            return False
        try:
            candidate = json.loads(line)
        except ValueError:  # JSONDecodeError and UnicodeDecodeError
            return False
        return isinstance(candidate, dict) and candidate.get("case_id") == self.case_id

    def _set_report_locked(self, line: bytes) -> None:
        # Later qualifying reports replace earlier ones; the slot is protected
        # from log eviction, so only tails give way when room is needed.
        self._report = line
        self._evict_locked(0, prefer_keep="stderr")
