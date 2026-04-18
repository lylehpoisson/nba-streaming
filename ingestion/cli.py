"""
NBA Pipeline CLI
Usage:
  python cli.py load --season 2024-25
  python cli.py load --season 2023-24
"""

import click
from load_to_snowflake import load as load_data

DEFAULT_SEASON = "2024-25"


@click.group()
def cli():
    """NBA data pipeline CLI."""
    pass


@cli.command()
@click.option(
    "--season",
    default=DEFAULT_SEASON,
    show_default=True,
    help="NBA season to load, e.g. 2023-24",
)
def load(season):
    """Load raw NBA data into Snowflake for a given season."""
    load_data(season)


# Future commands slot in here cleanly:
#
# @cli.command()
# @click.option("--start", required=True, help="Start season e.g. 2020-21")
# @click.option("--end", required=True, help="End season e.g. 2024-25")
# def backfill(start, end):
#     """Backfill multiple seasons."""
#     ...
#
# @cli.command()
# def validate():
#     """Run dbt tests and report results."""
#     ...


if __name__ == "__main__":
    cli()
