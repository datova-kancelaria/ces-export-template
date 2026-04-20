from __future__ import annotations

import logging
from collections import Counter


class DedupingWarningHandler(logging.Handler):
    def __init__(self, max_examples: int = 2) -> None:
        super().__init__(level=logging.WARNING)
        self.max_examples = max_examples
        self.counts: Counter[tuple[str, int, str]] = Counter()
        self.scope_label: str | None = None
        self.stream = logging.StreamHandler()
        self.stream.setFormatter(logging.Formatter("%(message)s"))

    def begin_scope(self, label: str) -> None:
        self.scope_label = label
        self.counts.clear()

    def end_scope(self) -> None:
        label = self.scope_label or "warnings"
        for (logger_name, levelno, msg), count in self.counts.items():
            suppressed = count - self.max_examples
            if suppressed > 0:
                print(
                    f"[{label}] {suppressed} more "
                    f"{logging.getLevelName(levelno)} message(s) suppressed "
                    f"from {logger_name}: {msg}"
                )
        self.counts.clear()
        self.scope_label = None

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        key = (record.name, record.levelno, msg)
        self.counts[key] += 1

        if self.counts[key] <= self.max_examples:
            compact = logging.makeLogRecord(record.__dict__.copy())
            compact.exc_info = None
            compact.exc_text = None
            self.stream.emit(compact)


rdflib_warning_handler = DedupingWarningHandler(max_examples=2)


def configure_rdflib_warning_suppression() -> None:
    logger = logging.getLogger("rdflib.term")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.WARNING)
    logger.addHandler(rdflib_warning_handler)
