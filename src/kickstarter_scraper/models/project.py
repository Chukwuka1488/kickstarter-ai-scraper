"""Pydantic models for Kickstarter project data.

Covers Kaggle-equivalent fields plus extras: description, rewards, creator, updates, comments.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Location(BaseModel):
    """Project or creator location."""

    name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None


class Creator(BaseModel):
    """Kickstarter project creator."""

    id: int
    name: str
    slug: Optional[str] = None
    url: Optional[str] = None
    avatar_url: Optional[str] = None
    location: Optional[Location] = None
    created_projects_count: Optional[int] = None
    backed_projects_count: Optional[int] = None
    is_verified: Optional[bool] = None
    biography: Optional[str] = None
    websites: list[str] = Field(default_factory=list)
    joined_at: Optional[str] = None


class RewardTier(BaseModel):
    """A single reward/pledge tier."""

    id: int
    title: Optional[str] = None
    description: Optional[str] = None
    minimum_pledge: float = 0.0
    currency: Optional[str] = None
    backers_count: int = 0
    estimated_delivery: Optional[str] = None
    limited: bool = False
    limit: Optional[int] = None
    remaining: Optional[int] = None
    shipping_type: Optional[str] = None


class Project(BaseModel):
    """Full Kickstarter project record.

    Kaggle-equivalent fields are marked with [K].
    Extra fields are marked with [E].
    """

    # === Identifiers ===
    id: int = Field(description="[K] Kickstarter project ID")
    slug: Optional[str] = Field(default=None, description="[K] URL slug")
    url: Optional[str] = Field(default=None, description="[K] Project URL")

    # === Core info (Kaggle-equivalent) ===
    name: str = Field(description="[K] Project name/title")
    blurb: Optional[str] = Field(default=None, description="[K] Short description (tagline)")
    category_name: Optional[str] = Field(default=None, description="[K] Category name")
    category_slug: Optional[str] = Field(default=None, description="[K] Category slug")
    category_parent: Optional[str] = Field(default=None, description="[K] Parent category")
    subcategory_name: Optional[str] = Field(default=None, description="[E] Subcategory name")

    # === Funding (Kaggle-equivalent) ===
    goal: float = Field(description="[K] Funding goal amount")
    pledged: float = Field(default=0.0, description="[K] Amount pledged")
    currency: Optional[str] = Field(default=None, description="[K] Currency code (USD, EUR, etc.)")
    usd_pledged: Optional[float] = Field(default=None, description="[K] Pledged amount in USD")
    usd_goal: Optional[float] = Field(default=None, description="[E] Goal in USD (converted)")
    fx_rate: Optional[float] = Field(default=None, description="[E] FX rate used for conversion")
    backers_count: int = Field(default=0, description="[K] Number of backers")
    state: str = Field(description="[K] Project state (live/successful/failed/canceled/suspended)")
    percent_funded: Optional[float] = Field(default=None, description="[E] Percentage of goal funded")

    # === Dates (Kaggle-equivalent) ===
    launched_at: Optional[datetime] = Field(default=None, description="[K] Launch datetime")
    deadline: Optional[datetime] = Field(default=None, description="[K] Funding deadline datetime")
    created_at: Optional[datetime] = Field(default=None, description="[K] Project creation datetime")
    state_changed_at: Optional[datetime] = Field(
        default=None, description="[K] Last state change datetime"
    )

    # === Location (Kaggle-equivalent) ===
    country: Optional[str] = Field(default=None, description="[K] Country code")
    location: Optional[Location] = Field(default=None, description="[E] Detailed location")

    # === Extra: Full description ===
    description: Optional[str] = Field(default=None, description="[E] Full project description")
    description_word_count: Optional[int] = Field(
        default=None, description="[E] Word count of description"
    )
    risks_and_challenges: Optional[str] = Field(
        default=None, description="[E] Risks & challenges text"
    )

    # === Extra: Media ===
    image_url: Optional[str] = Field(default=None, description="[E] Main project image URL")
    video_url: Optional[str] = Field(default=None, description="[E] Project video URL")
    has_video: bool = Field(default=False, description="[E] Whether project has a video")

    # === Extra: Engagement metrics ===
    comments_count: Optional[int] = Field(default=None, description="[E] Number of comments")
    updates_count: Optional[int] = Field(default=None, description="[E] Number of updates posted")
    watches_count: Optional[int] = Field(default=None, description="[E] Number of watchers (community proxy)")

    # === Extra: Campaign duration ===
    duration: Optional[int] = Field(default=None, description="[E] Campaign duration in days")

    # === Extra: Creator ===
    creator: Optional[Creator] = Field(default=None, description="[E] Creator profile")

    # === Extra: Rewards ===
    rewards: list[RewardTier] = Field(default_factory=list, description="[E] Reward tiers")
    reward_count: Optional[int] = Field(default=None, description="[E] Number of reward tiers")

    # === Extra: Tags/flags ===
    is_staff_pick: bool = Field(default=False, description="[E] Staff pick status")
    is_project_we_love: bool = Field(default=False, description="[E] 'Projects We Love' badge")
    spotlight: bool = Field(default=False, description="[E] Spotlight status")

    # === Metadata ===
    scraped_at: Optional[datetime] = Field(default=None, description="When this record was scraped")
    ai_relevance_score: Optional[float] = Field(
        default=None, description="AI-topic relevance score (0-1)"
    )

    def to_flat_dict(self) -> dict:
        """Flatten nested fields for CSV/DataFrame export."""
        d = self.model_dump()

        # Flatten location
        loc = d.pop("location", None) or {}
        d["location_name"] = loc.get("name")
        d["location_city"] = loc.get("city")
        d["location_state"] = loc.get("state")
        d["location_country"] = loc.get("country")

        # Flatten creator
        creator = d.pop("creator", None) or {}
        d["creator_id"] = creator.get("id")
        d["creator_name"] = creator.get("name")
        d["creator_slug"] = creator.get("slug")
        d["creator_projects_count"] = creator.get("created_projects_count")
        d["creator_backed_count"] = creator.get("backed_projects_count")
        d["creator_biography"] = creator.get("biography")
        d["creator_websites"] = "; ".join(creator.get("websites") or []) or None
        d["creator_joined_at"] = creator.get("joined_at")

        # Flatten creator location
        creator_loc = creator.get("location") or {}
        d["creator_location_name"] = creator_loc.get("name")
        d["creator_location_state"] = creator_loc.get("state")
        d["creator_location_country"] = creator_loc.get("country")

        # Summarize rewards
        rewards = d.pop("rewards", [])
        d["reward_count"] = len(rewards)
        d["reward_min_pledge"] = min((r["minimum_pledge"] for r in rewards), default=None)
        d["reward_max_pledge"] = max((r["minimum_pledge"] for r in rewards), default=None)
        d["reward_total_backers"] = sum(r["backers_count"] for r in rewards)

        return d
