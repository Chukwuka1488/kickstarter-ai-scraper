"""Tests for the Kickstarter API response parser."""

from kickstarter_scraper.api.parser import parse_project
from kickstarter_scraper.utils.relevance import compute_ai_relevance


SAMPLE_PROJECT = {
    "id": 123456,
    "slug": "ai-robot-companion",
    "name": "AI Robot Companion - Your Personal AI Assistant",
    "blurb": "A machine learning powered robot that understands you",
    "goal": 50000,
    "pledged": 75000,
    "currency": "USD",
    "usd_pledged": 75000,
    "backers_count": 500,
    "state": "successful",
    "launched_at": 1700000000,
    "deadline": 1703000000,
    "created_at": 1699000000,
    "country": "US",
    "staff_pick": True,
    "category": {
        "name": "Robots",
        "slug": "technology/robots",
        "parent_name": "Technology",
    },
    "creator": {
        "id": 789,
        "name": "Jane Doe",
        "slug": "janedoe",
        "urls": {"web": {"user": "https://www.kickstarter.com/profile/janedoe"}},
        "location": {
            "displayable_name": "San Francisco, CA",
            "name": "San Francisco",
            "state": "CA",
            "country": "US",
        },
    },
    "photo": {
        "full": "https://example.com/photo.jpg",
    },
    "video": {
        "high": "https://example.com/video.mp4",
        "base": "https://example.com/video_base.mp4",
    },
    "location": {
        "displayable_name": "San Francisco, CA",
        "name": "San Francisco",
        "state": "CA",
        "country": "US",
    },
    "comments_count": 42,
    "updates_count": 10,
    "urls": {
        "web": {
            "project": "https://www.kickstarter.com/projects/janedoe/ai-robot-companion",
        }
    },
}


def test_parse_project_basic_fields():
    project = parse_project(SAMPLE_PROJECT)
    assert project.id == 123456
    assert project.name == "AI Robot Companion - Your Personal AI Assistant"
    assert project.goal == 50000
    assert project.pledged == 75000
    assert project.backers_count == 500
    assert project.state == "successful"
    assert project.country == "US"
    assert project.is_staff_pick is True


def test_parse_project_category():
    project = parse_project(SAMPLE_PROJECT)
    assert project.category_name == "Robots"
    assert project.category_parent == "Technology"


def test_parse_project_creator():
    project = parse_project(SAMPLE_PROJECT)
    assert project.creator is not None
    assert project.creator.name == "Jane Doe"
    assert project.creator.id == 789


def test_parse_project_percent_funded():
    project = parse_project(SAMPLE_PROJECT)
    assert project.percent_funded == 150.0  # 75000/50000 * 100


def test_parse_project_location():
    project = parse_project(SAMPLE_PROJECT)
    assert project.location is not None
    # parse_location uses displayable_name for name, city comes from "city" key
    assert project.location.name == "San Francisco, CA"
    assert project.location.state == "CA"
    assert project.location.country == "US"


def test_parse_project_video():
    project = parse_project(SAMPLE_PROJECT)
    assert project.has_video is True
    assert project.video_url == "https://example.com/video.mp4"


def test_parse_project_creator_location():
    project = parse_project(SAMPLE_PROJECT)
    assert project.creator is not None
    assert project.creator.location is not None
    assert project.creator.location.name == "San Francisco, CA"
    assert project.creator.location.state == "CA"


def test_flat_dict():
    project = parse_project(SAMPLE_PROJECT)
    flat = project.to_flat_dict()
    assert "creator_name" in flat
    assert flat["creator_name"] == "Jane Doe"
    assert "location" not in flat  # should be flattened
    assert "creator" not in flat  # should be flattened


def test_flat_dict_new_fields():
    project = parse_project(SAMPLE_PROJECT)
    # Set new fields manually to test flattening
    project.creator.biography = "Robotics engineer"
    project.creator.websites = ["https://janedoe.com", "https://twitter.com/janedoe"]
    project.creator.joined_at = "2020-01-15"
    project.watches_count = 42
    project.duration = 30

    flat = project.to_flat_dict()
    assert flat["creator_biography"] == "Robotics engineer"
    assert flat["creator_websites"] == "https://janedoe.com; https://twitter.com/janedoe"
    assert flat["creator_joined_at"] == "2020-01-15"
    assert flat["creator_location_name"] == "San Francisco, CA"
    assert flat["creator_location_state"] == "CA"
    assert flat["creator_location_country"] == "US"
    assert flat["watches_count"] == 42
    assert flat["duration"] == 30


def test_ai_relevance_high():
    score = compute_ai_relevance(
        name="AI Assistant for Everyone",
        blurb="Your personal artificial intelligence companion",
    )
    assert score > 0.3


def test_ai_relevance_low():
    score = compute_ai_relevance(
        name="Organic Soap Bar",
        blurb="Handmade natural soap for sensitive skin",
    )
    assert score < 0.1


def test_ai_relevance_medium():
    score = compute_ai_relevance(
        name="Smart Home Controller",
        blurb="AI-powered home automation device",
    )
    assert score > 0.1
