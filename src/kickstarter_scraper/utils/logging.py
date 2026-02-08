"""Logging setup with Rich console output and file logging."""

from __future__ import annotations

import logging
from pathlib import Path

from rich.logging import RichHandler


def setup_logging(
    level: str = "INFO",
    log_dir: str = "logs",
    log_file: str = "scrape.log",
) -> logging.Logger:
    """Configure logging with both console (Rich) and file handlers.

    Args:
        level: Log level string.
        log_dir: Directory for log files.
        log_file: Log filename.

    Returns:
        Root logger.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Clear existing handlers
    root.handlers.clear()

    # Rich console handler
    console = RichHandler(
        level=logging.INFO,
        rich_tracebacks=True,
        show_time=True,
        show_path=False,
    )
    console.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(console)

    # File handler
    file_handler = logging.FileHandler(log_path / log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    root.addHandler(file_handler)

    return root
