# Kickstarter AI Project Scraper

Scrapes AI-related projects from Kickstarter with comprehensive data extraction: funding, creator profiles, locations, campaign content, rewards, and engagement metrics.

## Exported Fields (58 columns)

| Category | Fields |
|----------|--------|
| **Identifiers** | id, slug, name, blurb, url |
| **Category** | category_name, category_slug, category_parent |
| **Funding** | backers_count, goal, goal_currency, pledged, pledged_currency, currency, usd_pledged, fx_rate, percent_funded |
| **State** | state (live/successful/failed/canceled/suspended) |
| **Dates** | launched_at, deadline, created_at, state_changed_at, duration |
| **Project Location** | location_name, location_city, location_state, location_country, location_country_name |
| **Creator** | creator_id, creator_name, creator_slug, creator_url, creator_biography, creator_websites, creator_backed_count, creator_projects_count, creator_joined_at |
| **Creator Location** | creator_location_name, creator_location_state, creator_location_country, creator_location_country_name |
| **Engagement** | comments_count, updates_count, watches_count |
| **Video** | has_video, video_url |
| **Flags** | is_staff_pick, is_project_we_love, spotlight |
| **Campaign Content** | campaign_story_text, campaign_word_count, campaign_ai_mentions, risks, faqs, rewards, faq_count, reward_count |
| **Media** | image_url |

## Setup

```bash
git clone https://github.com/YOUR_USERNAME/kickstarter-ai-scraper.git
cd kickstarter-ai-scraper
pip install -e .
```

## Usage

The scraper runs in three stages:

### 1. Discovery - find AI projects

```bash
ks-scrape scrape
```

Searches Kickstarter's discovery API for AI-related projects across multiple search terms and categories. Saves raw JSON to `data/raw/projects.jsonl`.

### 2. Detail scrape - enrich with full data

```bash
ks-scrape details
```

Fetches expanded data for each project via GraphQL: creator bio/websites/location, campaign story, rewards, FAQs, funding details, video, and scrapes creator join date from profile pages. Saves to `data/raw/project_details.jsonl`.

Resumable - skips already-scraped projects automatically. To re-fetch everything with updated fields:

```bash
ks-scrape details --rescrape
```

### 3. Export - merge and output

```bash
ks-scrape export
```

Merges discovery + detail data into a single 58-column dataset. Outputs:
- `data/exports/kickstarter_ai_projects.csv`
- `data/exports/kickstarter_ai_projects.parquet`

### Quick stats

```bash
ks-scrape stats
```

## Configuration

Edit `configs/scrape_config.yaml` to adjust:

- **Search terms** and category filters
- **Rate limiting** (default: 1 req/sec)
- **Project states** to collect (live, successful, failed, canceled, suspended)
- **Output paths**

## Project Structure

```
src/kickstarter_scraper/
  api/
    client.py         # HTTP client (curl_cffi + Playwright fallback)
    parser.py         # JSON -> Pydantic model parsing
  models/
    project.py        # Project, Creator, Location, RewardTier models
  storage/
    json_store.py     # JSONL append-only storage with dedup
    export.py         # CSV/Parquet export
  utils/
    config.py         # YAML config loader
    logging.py        # Logging setup
    relevance.py      # AI-topic relevance scoring
  cli.py              # CLI entry point
  scraper.py          # Discovery scraper + merge/export
  detail_scraper.py   # GraphQL detail scraper + creator profile scraping
```
