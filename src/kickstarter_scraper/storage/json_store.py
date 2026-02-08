"""JSON-lines storage for raw scraped data with checkpoint support."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class JSONStore:
    """Append-only JSON-lines store for raw project data.

    Each line is a JSON object. Supports deduplication by project ID
    and checkpoint tracking for resumable scrapes.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._seen_ids: set[int] = set()

        # Load existing IDs for dedup
        if self.path.exists():
            self._load_existing_ids()

    def _load_existing_ids(self):
        """Scan existing file for project IDs."""
        try:
            with open(self.path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        pid = record.get("id")
                        if pid is not None:
                            self._seen_ids.add(pid)
                    except json.JSONDecodeError:
                        continue
            logger.info(f"Loaded {len(self._seen_ids)} existing project IDs from {self.path}")
        except OSError as e:
            logger.warning(f"Could not read existing store: {e}")

    def add(self, record: dict) -> bool:
        """Append a record if not already seen.

        Returns True if the record was added, False if duplicate.
        """
        pid = record.get("id")
        if pid in self._seen_ids:
            return False

        with open(self.path, "a") as f:
            f.write(json.dumps(record, default=str) + "\n")

        if pid is not None:
            self._seen_ids.add(pid)
        return True

    def add_many(self, records: list[dict]) -> int:
        """Append multiple records. Returns count of new records added."""
        added = 0
        for record in records:
            if self.add(record):
                added += 1
        return added

    def load_all(self) -> list[dict]:
        """Load all records from the store."""
        records = []
        if not self.path.exists():
            return records
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    @property
    def count(self) -> int:
        return len(self._seen_ids)


class Checkpoint:
    """Track scraping progress for resumable runs."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict = {}
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text())
            except (json.JSONDecodeError, OSError):
                self._data = {}

    def get(self, key: str, default=None):
        return self._data.get(key, default)

    def set(self, key: str, value):
        self._data[key] = value
        self._save()

    def get_completed_terms(self) -> set[str]:
        return set(self._data.get("completed_terms", []))

    def mark_term_done(self, term: str):
        completed = self.get_completed_terms()
        completed.add(term)
        self._data["completed_terms"] = list(completed)
        self._save()

    def get_last_page(self, term: str) -> int:
        return self._data.get("last_pages", {}).get(term, 0)

    def set_last_page(self, term: str, page: int):
        if "last_pages" not in self._data:
            self._data["last_pages"] = {}
        self._data["last_pages"][term] = page
        self._save()

    def clear(self):
        self._data = {}
        self._save()

    def _save(self):
        self.path.write_text(json.dumps(self._data, indent=2, default=str))
