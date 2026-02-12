"""Main scraper orchestrator - scrapes live AI projects from Kickstarter."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from kickstarter_scraper.api.client import KickstarterClient, KickstarterAPIError
from kickstarter_scraper.api.parser import parse_project
from kickstarter_scraper.models.project import Project
from kickstarter_scraper.storage.json_store import Checkpoint, JSONStore
from kickstarter_scraper.storage.export import export_csv, export_parquet

logger = logging.getLogger(__name__)


def mentions_ai(text: str) -> bool:
    """Check if 'AI' or 'artificial intelligence' appears in text."""
    if not text:
        return False
    lower = text.lower()
    return bool(re.search(r"\bai\b", lower) or "artificial intelligence" in lower)


async def _scrape_pages(
    client: KickstarterClient,
    store: JSONStore,
    term: str,
    state: str = "live",
    page_delay: float = 1.5,
    max_pages: int = 200,
) -> int:
    """Scrape paginated results, writing each page to store immediately."""
    added = 0
    page = 1
    consecutive_failures = 0

    while page <= max_pages:
        try:
            data = await client.discover(term=term, state=state, page=page)
            consecutive_failures = 0
        except KickstarterAPIError as e:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                logger.warning(
                    f"3 consecutive failures (term='{term}'), "
                    f"stopping at page {page}: {e}"
                )
                break
            logger.warning(f"Page {page} failed ({e}), waiting 30s")
            await asyncio.sleep(30)
            continue

        projects = data.get("projects", [])
        if not projects:
            break

        new = store.add_many(projects)
        added += new
        total = data.get("total_hits", 0)
        logger.info(
            f"Page {page}: {len(projects)} ({new} new) "
            f"| store={store.count} | hits={total}"
        )

        if not data.get("has_more", False):
            break

        page += 1
        if page_delay > 0:
            await asyncio.sleep(page_delay)

    return added


async def run_scrape(config: dict) -> list[Project]:
    """Scrape live AI projects from Kickstarter.

    Only fetches projects with state='live', then filters
    to those mentioning 'AI' in title or blurb.
    """
    scrape_cfg = config.get("scraping", {})
    output_cfg = config.get("output", {})

    raw_dir = Path(output_cfg.get("raw_dir", "data/raw"))
    export_dir = Path(output_cfg.get("export_dir", "data/exports"))

    store = JSONStore(raw_dir / "projects.jsonl")
    checkpoint = Checkpoint(output_cfg.get("checkpoint_file", "data/.checkpoint.json"))
    page_delay = scrape_cfg.get("page_delay", 1.5)

    async with KickstarterClient(
        rate_limit_rps=scrape_cfg.get("rate_limit_rps", 1.0),
        max_retries=scrape_cfg.get("max_retries", 3),
        timeout=scrape_cfg.get("timeout", 30),
    ) as client:
        completed = checkpoint.get_completed_terms()

        for state in ("live", "late"):
            key = f"AI|{state}"
            if key in completed:
                logger.info(f"Skipping completed: {key}")
                continue
            logger.info(f"Searching: term='AI' state='{state}'")
            await _scrape_pages(
                client, store, term="AI", state=state, page_delay=page_delay,
            )
            checkpoint.mark_term_done(key)

        logger.info(f"Collection done: {store.count} projects in store")

        # Parse all records
        all_records = store.load_all()
        now = datetime.now(timezone.utc)
        projects = [parse_project(raw, scraped_at=now) for raw in all_records]

    # Filter: keep projects mentioning AI in title or blurb
    ai_projects = [
        p for p in projects
        if mentions_ai(p.name) or mentions_ai(p.blurb or "")
    ]
    logger.info(f"AI filter: {len(ai_projects)}/{len(projects)} mention AI in title/blurb")

    # Export
    if ai_projects:
        export_csv(ai_projects, export_dir / "kickstarter_ai_projects.csv")
        export_parquet(ai_projects, export_dir / "kickstarter_ai_projects.parquet")

    return ai_projects


def _ts_to_iso(ts) -> str | None:
    """Convert a Unix timestamp to ISO date string."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return None


def merge_and_export(config: dict):
    """Merge discovery (projects.jsonl) and detail (project_details.jsonl) data
    into a comprehensive CSV/Parquet export with all available fields.
    """
    output_cfg = config.get("output", {})
    raw_dir = Path(output_cfg.get("raw_dir", "data/raw"))
    export_dir = Path(output_cfg.get("export_dir", "data/exports"))
    export_dir.mkdir(parents=True, exist_ok=True)

    # Load discovery data
    disc_store = JSONStore(raw_dir / "projects.jsonl")
    disc_records = disc_store.load_all()
    logger.info(f"Loaded {len(disc_records)} discovery records")

    # Index discovery records by slug
    disc_by_slug = {}
    for rec in disc_records:
        slug = rec.get("slug")
        if slug:
            disc_by_slug[slug] = rec

    # Load detail data
    detail_store = JSONStore(raw_dir / "project_details.jsonl")
    detail_records = detail_store.load_all()
    detail_by_slug = {}
    for rec in detail_records:
        slug = rec.get("slug")
        if slug:
            detail_by_slug[slug] = rec
    logger.info(f"Loaded {len(detail_records)} detail records")

    # Merge: start with discovery, enrich with detail
    rows = []
    all_slugs = set(disc_by_slug.keys()) | set(detail_by_slug.keys())

    for slug in all_slugs:
        disc = disc_by_slug.get(slug, {})
        detail = detail_by_slug.get(slug, {})

        # Discovery-sourced location
        disc_loc = disc.get("location") or {}
        disc_creator = disc.get("creator") or {}
        disc_video = disc.get("video") or {}
        disc_photo = disc.get("photo") or {}
        disc_category = disc.get("category") or {}
        disc_urls = disc.get("urls", {}).get("web", {})

        # Prefer detail data when available, fall back to discovery
        row = {
            # Identifiers
            "id": detail.get("id") or disc.get("id"),
            "slug": slug,
            "name": detail.get("name") or disc.get("name", ""),
            "blurb": disc.get("blurb"),
            "url": detail.get("url") or disc_urls.get("project"),

            # Category (discovery only)
            "category_name": disc_category.get("name"),
            "category_slug": disc_category.get("slug"),
            "category_parent": disc_category.get("parent_name"),

            # Funding - prefer detail (GraphQL) which has cleaner data
            "backers_count": detail.get("backers_count") if detail.get("backers_count") is not None else disc.get("backers_count", 0),
            "goal": detail.get("goal") or disc.get("goal"),
            "goal_currency": detail.get("goal_currency") or disc.get("currency"),
            "pledged": detail.get("pledged") or disc.get("pledged"),
            "pledged_currency": detail.get("pledged_currency") or disc.get("currency"),
            "currency": detail.get("goal_currency") or disc.get("currency"),
            "usd_pledged": disc.get("usd_pledged"),
            "fx_rate": disc.get("fx_rate"),
            "percent_funded": disc.get("percent_funded"),

            # State
            "state": detail.get("state") or disc.get("state", ""),

            # Dates - detail has epoch timestamps, discovery has epoch timestamps
            "launched_at": _ts_to_iso(detail.get("launched_at") or disc.get("launched_at")),
            "deadline": _ts_to_iso(detail.get("deadline_at") or disc.get("deadline")),
            "created_at": _ts_to_iso(disc.get("created_at")),
            "state_changed_at": _ts_to_iso(detail.get("state_changed_at") or disc.get("state_changed_at")),
            "duration": detail.get("duration"),

            # Project location - prefer detail (cleaner field names)
            "location_name": detail.get("location_name") or disc_loc.get("displayable_name") or disc_loc.get("short_name"),
            "location_city": detail.get("location_city") or disc_loc.get("name"),
            "location_state": detail.get("location_state") or disc_loc.get("state"),
            "location_country": detail.get("location_country") or disc_loc.get("country"),
            "location_country_name": detail.get("location_country_name") or disc_loc.get("expanded_country") or disc.get("country_displayable_name"),

            # Creator
            "creator_id": detail.get("creator_id") or disc_creator.get("id"),
            "creator_name": detail.get("creator_name") or disc_creator.get("name"),
            "creator_slug": detail.get("creator_slug") or disc_creator.get("slug"),
            "creator_url": detail.get("creator_url") or disc_creator.get("urls", {}).get("web", {}).get("user"),
            "creator_biography": detail.get("creator_biography"),
            "creator_websites": "; ".join(detail.get("creator_websites") or []) or None,
            "creator_backed_count": detail.get("creator_backed_count"),
            "creator_projects_count": detail.get("creator_projects_count"),
            "creator_joined_at": detail.get("creator_joined_at"),

            # Creator location
            "creator_location_name": detail.get("creator_location_name"),
            "creator_location_state": detail.get("creator_location_state"),
            "creator_location_country": detail.get("creator_location_country"),
            "creator_location_country_name": detail.get("creator_location_country_name"),

            # Engagement
            "comments_count": detail.get("comments_count") if detail.get("comments_count") is not None else disc.get("comments_count"),
            "updates_count": detail.get("updates_count") if detail.get("updates_count") is not None else disc.get("updates_count"),
            "watches_count": detail.get("watches_count"),

            # Video
            "has_video": detail.get("has_video") if "has_video" in detail else (disc_video is not None and bool(disc_video)),
            "video_url": detail.get("video_url") or (disc_video.get("high") or disc_video.get("base") if isinstance(disc_video, dict) else None),

            # Flags
            "is_staff_pick": bool(disc.get("staff_pick")),
            "is_project_we_love": detail.get("is_project_we_love", False),
            "spotlight": bool(disc.get("spotlight")),

            # Detail-only content
            "faq_count": detail.get("faq_count"),
            "reward_count": detail.get("reward_count"),
            "campaign_story_text": detail.get("campaign_story_text"),
            "campaign_word_count": detail.get("campaign_word_count"),
            "campaign_ai_mentions": detail.get("campaign_ai_mentions"),
            "risks": detail.get("risks"),
            "faqs": json.dumps(detail.get("faqs")) if detail.get("faqs") else None,
            "rewards": json.dumps(detail.get("rewards")) if detail.get("rewards") else None,

            # Discovery image
            "image_url": disc_photo.get("full") or disc_photo.get("med") or disc_photo.get("1024x576"),
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    # Filter to AI-related projects
    mask = df["name"].str.contains(r"\bai\b|artificial intelligence", case=False, na=False) | \
           df["blurb"].fillna("").str.contains(r"\bai\b|artificial intelligence", case=False, na=False)
    df_ai = df[mask].copy()
    logger.info(f"AI filter: {len(df_ai)}/{len(df)} projects")

    # Export
    csv_path = export_dir / "kickstarter_ai_projects.csv"
    parquet_path = export_dir / "kickstarter_ai_projects.parquet"

    df_ai.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"Exported {len(df_ai)} projects to CSV: {csv_path}")

    df_ai.to_parquet(parquet_path, index=False, engine="pyarrow")
    logger.info(f"Exported {len(df_ai)} projects to Parquet: {parquet_path}")

    # Print summary of field coverage
    logger.info("=== Field coverage ===")
    for col in df_ai.columns:
        filled = df_ai[col].notna().sum()
        pct = filled / len(df_ai) * 100 if len(df_ai) > 0 else 0
        if pct < 100:
            logger.info(f"  {col:35s}: {filled}/{len(df_ai)} ({pct:.0f}%)")

    return df_ai
