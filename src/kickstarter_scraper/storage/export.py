"""Export scraped data to CSV and Parquet formats."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from kickstarter_scraper.models.project import Project

logger = logging.getLogger(__name__)


def projects_to_dataframe(projects: list[Project]) -> pd.DataFrame:
    """Convert a list of Project models to a flat DataFrame."""
    if not projects:
        return pd.DataFrame()

    rows = [p.to_flat_dict() for p in projects]
    df = pd.DataFrame(rows)

    # Convert datetime columns
    dt_cols = [
        "launched_at", "deadline", "created_at",
        "state_changed_at", "scraped_at",
    ]
    for col in dt_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    return df


def export_csv(
    projects: list[Project],
    path: str | Path,
    include_description: bool = False,
) -> Path:
    """Export projects to CSV.

    Args:
        projects: List of Project models.
        path: Output CSV file path.
        include_description: Whether to include full description text (large).

    Returns:
        Path to the written CSV file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = projects_to_dataframe(projects)

    if not include_description and "description" in df.columns:
        df = df.drop(columns=["description", "risks_and_challenges"], errors="ignore")

    df.to_csv(path, index=False, encoding="utf-8")
    logger.info(f"Exported {len(df)} projects to CSV: {path}")
    return path


def export_parquet(
    projects: list[Project],
    path: str | Path,
) -> Path:
    """Export projects to Parquet (includes all fields).

    Args:
        projects: List of Project models.
        path: Output Parquet file path.

    Returns:
        Path to the written Parquet file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = projects_to_dataframe(projects)
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"Exported {len(df)} projects to Parquet: {path}")
    return path


def load_parquet(path: str | Path) -> pd.DataFrame:
    """Load a Parquet file into a DataFrame."""
    return pd.read_parquet(path, engine="pyarrow")
