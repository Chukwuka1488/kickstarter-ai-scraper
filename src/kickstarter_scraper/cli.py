"""CLI entry point for the Kickstarter AI scraper."""

from __future__ import annotations

import asyncio
import sys

import click
from rich.console import Console

from kickstarter_scraper.scraper import run_scrape, merge_and_export
from kickstarter_scraper.detail_scraper import run_detail_scrape
from kickstarter_scraper.utils.config import load_config
from kickstarter_scraper.utils.logging import setup_logging

console = Console()


@click.group()
def main():
    """Kickstarter AI Project Scraper."""


@main.command()
@click.option(
    "--config", "-c",
    default="configs/scrape_config.yaml",
    help="Path to config file.",
)
@click.option(
    "--log-level", "-l",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
)
def scrape(config: str, log_level: str):
    """Run the full scrape pipeline."""
    setup_logging(level=log_level)
    cfg = load_config(config)
    projects = asyncio.run(run_scrape(cfg))
    console.print(f"\n[bold green]Done![/] Scraped {len(projects)} AI-related projects.")


@main.command()
@click.option("--config", "-c", default="configs/scrape_config.yaml")
@click.option("--log-level", "-l", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
@click.option("--rescrape", is_flag=True, default=False,
              help="Clear existing detail data and re-fetch everything with expanded fields.")
def details(config: str, log_level: str, rescrape: bool):
    """Scrape full details (story, rewards, FAQs, creator, location, etc.) via GraphQL."""
    setup_logging(level=log_level)
    cfg = load_config(config)
    run_detail_scrape(cfg, rescrape=rescrape)
    console.print("[bold green]Done![/] Detail scrape complete.")


@main.command(name="export")
@click.option("--config", "-c", default="configs/scrape_config.yaml")
@click.option("--log-level", "-l", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]))
def export_cmd(config: str, log_level: str):
    """Merge discovery + detail data and export comprehensive CSV/Parquet."""
    setup_logging(level=log_level)
    cfg = load_config(config)
    df = merge_and_export(cfg)
    console.print(f"[bold green]Done![/] Exported {len(df)} projects with all available fields.")


@main.command()
@click.argument("parquet_path", default="data/exports/kickstarter_ai_projects.parquet")
def stats(parquet_path: str):
    """Show quick stats from exported data."""
    import pandas as pd

    try:
        df = pd.read_parquet(parquet_path)
    except FileNotFoundError:
        console.print(f"[red]File not found:[/] {parquet_path}")
        console.print("Run 'ks-scrape scrape' first.")
        sys.exit(1)

    console.print(f"\n[bold]Kickstarter AI Projects Summary[/]")
    console.print(f"Total projects: {len(df)}")
    console.print(f"States: {df['state'].value_counts().to_dict()}")
    console.print(f"Total pledged (USD): ${df['usd_pledged'].sum():,.0f}")
    console.print(f"Total backers: {df['backers_count'].sum():,}")
    console.print(f"Categories: {df['category_name'].nunique()}")

    if "launched_at" in df.columns:
        console.print(f"Date range: {df['launched_at'].min()} to {df['launched_at'].max()}")


if __name__ == "__main__":
    main()
