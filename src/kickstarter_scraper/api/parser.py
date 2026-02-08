"""Parse raw Kickstarter JSON responses into Pydantic models."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from kickstarter_scraper.models.project import Creator, Location, Project, RewardTier

logger = logging.getLogger(__name__)


def _ts_to_dt(ts: Any) -> Optional[datetime]:
    """Convert a Unix timestamp to datetime, or return None."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def parse_location(data: Optional[dict]) -> Optional[Location]:
    """Parse location from Kickstarter JSON."""
    if not data:
        return None
    return Location(
        name=data.get("displayable_name") or data.get("name"),
        city=data.get("city"),
        state=data.get("state"),
        country=data.get("country"),
        country_code=data.get("short_name") or data.get("country"),
    )


def parse_creator(data: Optional[dict]) -> Optional[Creator]:
    """Parse creator profile from Kickstarter JSON."""
    if not data:
        return None
    return Creator(
        id=data.get("id", 0),
        name=data.get("name", "Unknown"),
        slug=data.get("slug"),
        url=data.get("urls", {}).get("web", {}).get("user"),
        avatar_url=data.get("avatar", {}).get("medium"),
        location=parse_location(data.get("location")),
        created_projects_count=data.get("created_projects_count"),
        backed_projects_count=data.get("backed_projects_count"),
        is_verified=data.get("is_registered"),
    )


def parse_reward(data: dict) -> RewardTier:
    """Parse a single reward tier from Kickstarter JSON."""
    return RewardTier(
        id=data.get("id", 0),
        title=data.get("title"),
        description=data.get("description"),
        minimum_pledge=data.get("minimum", 0),
        currency=data.get("currency"),
        backers_count=data.get("backers_count", 0),
        estimated_delivery=data.get("estimated_delivery"),
        limited=bool(data.get("limit")),
        limit=data.get("limit"),
        remaining=data.get("remaining"),
        shipping_type=data.get("shipping_type"),
    )


def parse_project(data: dict, scraped_at: Optional[datetime] = None) -> Project:
    """Parse a project from Kickstarter API JSON.

    Handles both discovery list items and full project detail responses.

    Args:
        data: Raw project dict from Kickstarter JSON.
        scraped_at: Timestamp when this was scraped.

    Returns:
        Validated Project model.
    """
    # Category can be nested
    category = data.get("category", {}) or {}
    parent_category = category.get("parent_name") or category.get("parent", {}).get("name")

    # URL construction
    urls = data.get("urls", {}).get("web", {})
    project_url = urls.get("project") if urls else None

    # Creator
    creator = parse_creator(data.get("creator"))

    # Rewards (may or may not be present)
    raw_rewards = data.get("rewards", {}).get("rewards", []) if isinstance(
        data.get("rewards"), dict
    ) else data.get("rewards", [])
    rewards = [parse_reward(r) for r in (raw_rewards or []) if isinstance(r, dict)]

    # Video detection
    video = data.get("video")
    has_video = video is not None and bool(video)
    video_url = None
    if isinstance(video, dict):
        video_url = video.get("high") or video.get("base")

    # Photo/image
    photo = data.get("photo", {}) or {}
    image_url = photo.get("full") or photo.get("med") or photo.get("1024x576")

    # USD conversion
    usd_type = data.get("usd_type")
    converted = data.get("converted_pledged_amount")
    usd_pledged = data.get("usd_pledged")
    if usd_pledged is not None:
        usd_pledged = float(usd_pledged)
    elif converted:
        usd_pledged = float(converted)

    # Percent funded
    goal = float(data.get("goal", 0))
    pledged = float(data.get("pledged", 0))
    percent_funded = (pledged / goal * 100) if goal > 0 else 0.0

    return Project(
        id=data["id"],
        slug=data.get("slug"),
        url=project_url,
        name=data.get("name", ""),
        blurb=data.get("blurb"),
        category_name=category.get("name"),
        category_slug=category.get("slug"),
        category_parent=parent_category,
        subcategory_name=category.get("name") if parent_category else None,
        goal=goal,
        pledged=pledged,
        currency=data.get("currency"),
        usd_pledged=usd_pledged,
        fx_rate=data.get("fx_rate"),
        backers_count=data.get("backers_count", 0),
        state=data.get("state", "unknown"),
        percent_funded=percent_funded,
        launched_at=_ts_to_dt(data.get("launched_at")),
        deadline=_ts_to_dt(data.get("deadline")),
        created_at=_ts_to_dt(data.get("created_at")),
        state_changed_at=_ts_to_dt(data.get("state_changed_at")),
        country=data.get("country"),
        location=parse_location(data.get("location")),
        description=data.get("description"),
        description_word_count=(
            len(data["description"].split()) if data.get("description") else None
        ),
        risks_and_challenges=data.get("risks"),
        image_url=image_url,
        video_url=video_url,
        has_video=has_video,
        comments_count=data.get("comments_count"),
        updates_count=data.get("updates_count"),
        creator=creator,
        rewards=rewards,
        reward_count=len(rewards),
        is_staff_pick=bool(data.get("staff_pick")),
        is_project_we_love=bool(data.get("is_project_we_love")),
        spotlight=bool(data.get("spotlight")),
        scraped_at=scraped_at or datetime.now(timezone.utc),
    )
