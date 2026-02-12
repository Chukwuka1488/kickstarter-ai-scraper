"""Per-project detail scraper using curl_cffi + Kickstarter GraphQL.

Reads project slugs from the CSV, fetches full details via GraphQL
(story, risks, FAQs, rewards, comments, updates, creator profile,
location, funding, dates, video), and writes to a separate JSONL file.

Fields extracted:
  1. URL + identifiers
  2. Funding: backers, goal, pledged (with currency), state
  3. Dates: launched, deadline, duration
  4. Location: project city/state/country
  5. Creator: name, bio, websites, location, backed/created counts, join date
  6. Engagement: comments, updates, watches (community proxy)
  7. Campaign story (full HTML + plain text) + AI mention count
  8. Risks and challenges
  9. Rewards (name, amount, description, backers, delivery)
  10. FAQ (question + answer pairs)
  11. Video availability + URL
  12. Flags: isProjectWeLove
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

import pandas as pd
from curl_cffi import requests as curl_requests

from kickstarter_scraper.storage.json_store import JSONStore

logger = logging.getLogger(__name__)

BASE_URL = "https://www.kickstarter.com"

GRAPHQL_QUERY = """query Project($slug: String!) {
  project(slug: $slug) {
    story(assetWidth: 680)
    risks
    description
    backersCount
    goal { amount currency symbol }
    pledged { amount currency symbol }
    state
    stateChangedAt
    launchedAt
    deadlineAt
    duration
    location { displayableName name country countryName state }
    creator {
      id
      name
      slug
      url
      imageUrl(width: 80)
      biography
      websites { url }
      backingsCount
      launchedProjects { totalCount }
      location { displayableName name country countryName state }
    }
    commentsCount
    posts { totalCount }
    watchesCount
    video { videoSources { high { src } } }
    isProjectWeLove
    faqs { nodes { question answer } }
    rewards {
      nodes {
        name
        description
        amount { amount currency }
        backersCount
        estimatedDeliveryOn
      }
    }
  }
}"""


def _clean_html(html: str) -> str:
    """Strip HTML tags, collapse whitespace."""
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def _count_ai(text: str) -> int:
    """Count 'AI' or 'artificial intelligence' mentions."""
    if not text:
        return 0
    return len(re.findall(r"\bAI\b", text)) + len(
        re.findall(r"\bartificial intelligence\b", text, re.IGNORECASE)
    )


def _get_session_and_csrf(project_url: str, retries: int = 3) -> tuple[curl_requests.Session, str]:
    """Visit a project page to get session cookies and CSRF token.

    Retries with exponential backoff if the page doesn't return a CSRF token
    (e.g., Cloudflare challenge or rate-limit block).
    """
    for attempt in range(retries):
        try:
            session = curl_requests.Session()
            resp = session.get(project_url, impersonate="chrome", timeout=30)
            csrf = ""
            m = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', resp.text)
            if m:
                csrf = m.group(1)
                return session, csrf
            # No CSRF found — might be a Cloudflare challenge page
            logger.warning(f"No CSRF token (attempt {attempt + 1}/{retries}, status={resp.status_code})")
        except Exception as e:
            logger.warning(f"Session init failed (attempt {attempt + 1}/{retries}): {e}")
        backoff = 30 * (2 ** attempt)  # 30s, 60s, 120s
        logger.info(f"Waiting {backoff}s before retry...")
        time.sleep(backoff)
    return session, ""


def _scrape_creator_joined_at(session: curl_requests.Session, creator_slug: str) -> str | None:
    """Scrape creator join date from their profile page HTML.

    The join date appears as: <meta property="joined" content="YYYY-MM-DD ..."/>
    or near a 'Joined' label with <time datetime="YYYY-MM-DD">.
    """
    if not creator_slug:
        return None
    try:
        resp = session.get(
            f"{BASE_URL}/profile/{creator_slug}",
            impersonate="chrome",
            timeout=30,
        )
        if resp.status_code != 200:
            return None
        # Look for meta property="joined" content="..."
        m = re.search(r'property="joined"\s+content="([^"]+)"', resp.text)
        if m:
            # Extract just the date portion (YYYY-MM-DD)
            raw = m.group(1).strip()
            date_match = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
            return date_match.group(1) if date_match else raw
        # Fallback: look for <time datetime="..."> near "Joined"
        m = re.search(r'Joined.*?<time\s+datetime="([^"]+)"', resp.text, re.DOTALL)
        if m:
            raw = m.group(1).strip()
            date_match = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
            return date_match.group(1) if date_match else raw
    except Exception as e:
        logger.debug(f"Could not scrape joined_at for {creator_slug}: {e}")
    return None


def _fetch_project_graphql(
    session: curl_requests.Session, csrf: str, project_slug: str
) -> dict | None:
    """Fetch project details via GraphQL."""
    try:
        resp = session.post(
            f"{BASE_URL}/graph",
            json={"query": GRAPHQL_QUERY, "variables": {"slug": project_slug}},
            headers={"X-CSRF-Token": csrf, "Content-Type": "application/json"},
            impersonate="chrome",
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(f"GraphQL {resp.status_code} for {project_slug}")
            return None
        data = resp.json()
        if "errors" in data:
            logger.warning(f"GraphQL errors for {project_slug}: {data['errors'][0].get('message', '')[:100]}")
            return None
        return data.get("data", {}).get("project")
    except Exception as e:
        logger.warning(f"GraphQL request failed for {project_slug}: {type(e).__name__}: {e}")
        return None


def run_detail_scrape(config: dict, rescrape: bool = False):
    """Scrape full details for all projects in the CSV.

    Reads from data/exports/kickstarter_ai_projects.csv,
    writes to data/raw/project_details.jsonl.

    Args:
        config: Scraper configuration dict.
        rescrape: If True, clear existing detail data and re-fetch everything.
    """
    output_cfg = config.get("output", {})
    scrape_cfg = config.get("scraping", {})
    raw_dir = Path(output_cfg.get("raw_dir", "data/raw"))
    export_dir = Path(output_cfg.get("export_dir", "data/exports"))
    page_delay = scrape_cfg.get("page_delay", 2.0)

    csv_path = export_dir / "kickstarter_ai_projects.csv"
    if not csv_path.exists():
        logger.error(f"CSV not found: {csv_path}. Run 'ks-scrape scrape' first.")
        return

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} projects from {csv_path}")

    detail_path = raw_dir / "project_details.jsonl"
    if rescrape and detail_path.exists():
        backup = raw_dir / "project_details_old.jsonl"
        logger.info(f"Rescrape mode: backing up {detail_path} -> {backup}")
        import shutil
        shutil.copy2(detail_path, backup)
        detail_path.unlink()

    detail_store = JSONStore(detail_path)
    already_scraped = {r.get("slug") for r in detail_store.load_all()}
    logger.info(f"Already scraped: {len(already_scraped)} projects")

    # Get session + CSRF token from first unskipped project
    unskipped = df[~df["slug"].isin(already_scraped)]
    if unskipped.empty:
        logger.info("All projects already scraped.")
        return
    first_url = unskipped["url"].iloc[0]
    session, csrf = _get_session_and_csrf(first_url)
    if not csrf:
        logger.error("Could not get CSRF token after retries. Aborting.")
        return
    logger.info("Got session cookies and CSRF token")

    # Refresh CSRF every N requests (session may expire)
    csrf_refresh_interval = 50
    success = 0
    failed = 0
    consecutive_fails = 0

    for i, row in df.iterrows():
        slug = row["slug"]
        project_url = row["url"]
        if slug in already_scraped:
            continue

        try:
            logger.info(f"[{i+1}/{len(df)}] {row['name'][:60]}")

            # Refresh session periodically
            if success > 0 and success % csrf_refresh_interval == 0:
                logger.info("Refreshing CSRF token...")
                session, csrf = _get_session_and_csrf(project_url)
                time.sleep(2)

            project_data = _fetch_project_graphql(session, csrf, slug)

            if project_data is None:
                failed += 1
                consecutive_fails += 1

                # Exponential backoff: 10s, 30s, 60s, 120s, then cap at 120s
                backoff = min(10 * (2 ** (consecutive_fails - 1)), 120)

                if consecutive_fails >= 2:
                    logger.info(f"Consecutive fail #{consecutive_fails}, refreshing session, waiting {backoff}s...")
                    time.sleep(backoff)
                    session, csrf = _get_session_and_csrf(project_url)
                    time.sleep(3)
                    project_data = _fetch_project_graphql(session, csrf, slug)

                if consecutive_fails >= 10:
                    logger.warning(f"10 consecutive failures, pausing 5 minutes...")
                    time.sleep(300)
                    session, csrf = _get_session_and_csrf(project_url)
                    consecutive_fails = 0

                if project_data is None:
                    logger.warning(f"Failed to fetch {slug}")
                    time.sleep(page_delay)
                    continue
        except KeyboardInterrupt:
            logger.info(f"Interrupted. Saved {success} projects so far.")
            break
        except Exception as e:
            logger.warning(f"Unexpected error on {slug}: {type(e).__name__}: {e}")
            failed += 1
            consecutive_fails += 1
            time.sleep(page_delay)
            continue

        # Build detail record (outside try so failures above skip this)
        try:
            story_html = project_data.get("story", "")
            story_text = _clean_html(story_html)
            faqs = project_data.get("faqs", {}).get("nodes", [])
            rewards = project_data.get("rewards", {}).get("nodes", [])
            creator_data = project_data.get("creator") or {}
            location_data = project_data.get("location") or {}
            creator_loc = creator_data.get("location") or {}
            goal_data = project_data.get("goal") or {}
            pledged_data = project_data.get("pledged") or {}
            video_data = project_data.get("video") or {}
            video_src = (video_data.get("videoSources") or {}).get("high", {}).get("src")

            consecutive_fails = 0  # Reset on success

            # Scrape creator join date from profile page (with small delay to avoid rate-limit)
            creator_slug = creator_data.get("slug", "")
            time.sleep(1.5)
            creator_joined = _scrape_creator_joined_at(session, creator_slug)

            detail = {
                "id": row["id"],
                "slug": slug,
                "name": row["name"],
                "url": row["url"],
                # Funding
                "backers_count": project_data.get("backersCount", 0),
                "goal": goal_data.get("amount"),
                "goal_currency": goal_data.get("currency"),
                "pledged": pledged_data.get("amount"),
                "pledged_currency": pledged_data.get("currency"),
                "state": project_data.get("state", ""),
                # Dates
                "launched_at": project_data.get("launchedAt"),
                "deadline_at": project_data.get("deadlineAt"),
                "state_changed_at": project_data.get("stateChangedAt"),
                "duration": project_data.get("duration"),
                # Project location
                "location_name": location_data.get("displayableName"),
                "location_city": location_data.get("name"),
                "location_state": location_data.get("state"),
                "location_country": location_data.get("country"),
                "location_country_name": location_data.get("countryName"),
                # Creator
                "creator_id": creator_data.get("id"),
                "creator_name": creator_data.get("name"),
                "creator_slug": creator_slug,
                "creator_url": creator_data.get("url"),
                "creator_biography": creator_data.get("biography"),
                "creator_websites": [w.get("url", "") for w in (creator_data.get("websites") or [])],
                "creator_backed_count": creator_data.get("backingsCount"),
                "creator_projects_count": (creator_data.get("launchedProjects") or {}).get("totalCount"),
                "creator_joined_at": creator_joined,
                # Creator location
                "creator_location_name": creator_loc.get("displayableName"),
                "creator_location_state": creator_loc.get("state"),
                "creator_location_country": creator_loc.get("country"),
                "creator_location_country_name": creator_loc.get("countryName"),
                # Engagement
                "comments_count": project_data.get("commentsCount", 0),
                "updates_count": project_data.get("posts", {}).get("totalCount", 0),
                "watches_count": project_data.get("watchesCount", 0),
                "faq_count": len(faqs),
                "reward_count": len(rewards),
                # Video
                "has_video": video_src is not None,
                "video_url": video_src,
                # Flags
                "is_project_we_love": project_data.get("isProjectWeLove", False),
                # Campaign content
                "campaign_story_html": story_html,
                "campaign_story_text": story_text,
                "campaign_word_count": len(story_text.split()),
                "campaign_ai_mentions": _count_ai(story_text),
                # Risks
                "risks": project_data.get("risks", ""),
                # FAQs
                "faqs": [{"question": f["question"], "answer": f["answer"]} for f in faqs],
                # Rewards
                "rewards": [
                    {
                        "name": r.get("name", ""),
                        "description": r.get("description", ""),
                        "amount": r.get("amount", {}).get("amount", ""),
                        "currency": r.get("amount", {}).get("currency", ""),
                        "backers": r.get("backersCount", 0),
                        "delivery": r.get("estimatedDeliveryOn", ""),
                    }
                    for r in rewards
                ],
            }

            detail_store.add(detail)
            already_scraped.add(slug)
            success += 1
        except KeyboardInterrupt:
            logger.info(f"Interrupted. Saved {success} projects so far.")
            break
        except Exception as e:
            logger.warning(f"Error building record for {slug}: {type(e).__name__}: {e}")
            failed += 1

        time.sleep(page_delay)

    logger.info(f"Detail scrape done: {success} new, {failed} failed, {detail_store.count} total")
