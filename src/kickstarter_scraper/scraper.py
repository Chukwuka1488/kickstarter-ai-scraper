"""Main scraper orchestrator - runs the full pipeline."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from kickstarter_scraper.api.client import KickstarterClient
from kickstarter_scraper.api.parser import parse_project
from kickstarter_scraper.models.project import Project
from kickstarter_scraper.storage.json_store import Checkpoint, JSONStore
from kickstarter_scraper.storage.export import export_csv, export_parquet
from kickstarter_scraper.utils.relevance import compute_ai_relevance

logger = logging.getLogger(__name__)


async def run_scrape(config: dict) -> list[Project]:
    """Execute the full scrape pipeline.

    1. Search Kickstarter for each term in config
    2. Optionally fetch full project details
    3. Score AI relevance
    4. Store raw JSON, export CSV + Parquet

    Args:
        config: Parsed config dict from scrape_config.yaml.

    Returns:
        List of parsed Project models.
    """
    search_cfg = config.get("search", {})
    scrape_cfg = config.get("scraping", {})
    output_cfg = config.get("output", {})
    filter_cfg = config.get("filtering", {})

    raw_dir = Path(output_cfg.get("raw_dir", "data/raw"))
    export_dir = Path(output_cfg.get("export_dir", "data/exports"))

    store = JSONStore(raw_dir / "projects.jsonl")
    checkpoint = Checkpoint(output_cfg.get("checkpoint_file", "data/.checkpoint.json"))

    terms = search_cfg.get("terms", ["artificial intelligence"])
    category_ids = search_cfg.get("category_ids", [])
    search_all = search_cfg.get("search_all_categories", True)
    min_relevance = filter_cfg.get("min_relevance_score", 0.3)

    async with KickstarterClient(
        rate_limit_rps=scrape_cfg.get("rate_limit_rps", 1.0),
        max_retries=scrape_cfg.get("max_retries", 3),
        timeout=scrape_cfg.get("timeout", 30),
        user_agent=scrape_cfg.get("user_agent", "KickstarterResearchBot/0.1"),
    ) as client:
        # Phase 1: Discovery search
        completed_terms = checkpoint.get_completed_terms()
        all_raw: list[dict] = []

        for term in terms:
            if term in completed_terms:
                logger.info(f"Skipping completed term: {term}")
                continue

            logger.info(f"Searching: '{term}'")

            if search_all:
                # Search across all categories
                projects = await client.discover_all_pages(
                    term=term,
                    page_delay=scrape_cfg.get("page_delay", 1.5),
                )
                all_raw.extend(projects)

            # Also search within specific categories
            for cat_id in category_ids:
                projects = await client.discover_all_pages(
                    term=term,
                    category_id=cat_id,
                    page_delay=scrape_cfg.get("page_delay", 1.5),
                )
                all_raw.extend(projects)

            checkpoint.mark_term_done(term)

        # Store raw data (deduped)
        added = store.add_many(all_raw)
        logger.info(f"Added {added} new projects ({store.count} total in store)")

        # Phase 2: Fetch full details (if enabled)
        fetch_details = scrape_cfg.get("fetch_project_details", False)
        all_records = store.load_all()

        now = datetime.now(timezone.utc)
        projects: list[Project] = []

        for raw in all_records:
            slug = raw.get("slug")

            if fetch_details and slug and "description" not in raw:
                try:
                    detail = await client.get_project(slug)
                    raw.update(detail)
                except Exception as e:
                    logger.warning(f"Failed to fetch details for {slug}: {e}")

            project = parse_project(raw, scraped_at=now)

            # Score AI relevance
            project.ai_relevance_score = compute_ai_relevance(
                name=project.name,
                blurb=project.blurb or "",
                description=project.description or "",
            )
            projects.append(project)

    # Phase 3: Filter by relevance
    relevant = [p for p in projects if (p.ai_relevance_score or 0) >= min_relevance]
    logger.info(
        f"Filtered to {len(relevant)}/{len(projects)} projects "
        f"with AI relevance >= {min_relevance}"
    )

    # Phase 4: Export
    if relevant:
        export_csv(relevant, export_dir / "kickstarter_ai_projects.csv")
        export_parquet(relevant, export_dir / "kickstarter_ai_projects.parquet")

        # Also export unfiltered for reference
        export_csv(projects, export_dir / "kickstarter_all_scraped.csv")

    return relevant
