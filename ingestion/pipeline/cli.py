"""
NBA Pipeline CLI
Usage:
  python cli.py load --season 2024-25
  python cli.py load --season 2023-24
"""

import click
from pipeline.load import load as load_data

DEFAULT_SEASON = "2025-26"


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


@cli.command()
def embed():
    """Generate embeddings from mart tables and load to Snowflake."""
    from pipeline.rag import embed_all

    embed_all()


@cli.command()
@click.argument("question")
@click.option(
    "--top-k",
    default=5,
    show_default=True,
    help="Number of similar entities to retrieve",
)
def query(question, top_k):
    """Ask a natural language question about NBA teams and players."""
    from pipeline.rag import query as rag_query

    answer = rag_query(question, top_k=top_k)
    click.echo(answer)


if __name__ == "__main__":
    cli()
