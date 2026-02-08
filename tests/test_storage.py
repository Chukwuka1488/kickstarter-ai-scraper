"""Tests for JSON store and checkpoint."""

import json
from pathlib import Path

from kickstarter_scraper.storage.json_store import JSONStore, Checkpoint


def test_json_store_add_and_dedup(tmp_path):
    store = JSONStore(tmp_path / "test.jsonl")
    assert store.add({"id": 1, "name": "Project A"}) is True
    assert store.add({"id": 2, "name": "Project B"}) is True
    assert store.add({"id": 1, "name": "Project A"}) is False  # duplicate
    assert store.count == 2


def test_json_store_load(tmp_path):
    store = JSONStore(tmp_path / "test.jsonl")
    store.add({"id": 1, "name": "A"})
    store.add({"id": 2, "name": "B"})
    records = store.load_all()
    assert len(records) == 2
    assert records[0]["name"] == "A"


def test_json_store_resume(tmp_path):
    path = tmp_path / "test.jsonl"
    store1 = JSONStore(path)
    store1.add({"id": 1})
    store1.add({"id": 2})

    # New instance loads existing IDs
    store2 = JSONStore(path)
    assert store2.count == 2
    assert store2.add({"id": 1}) is False
    assert store2.add({"id": 3}) is True


def test_checkpoint(tmp_path):
    cp = Checkpoint(tmp_path / "cp.json")
    cp.mark_term_done("AI")
    cp.mark_term_done("machine learning")
    assert "AI" in cp.get_completed_terms()

    # Reload
    cp2 = Checkpoint(tmp_path / "cp.json")
    assert cp2.get_completed_terms() == {"AI", "machine learning"}
