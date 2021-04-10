#!/usr/bin/env python

"""
Entry point for running vaccine feed runners
"""
import click
import dotenv


@click.group()
def cli():
    """Run vaccine-feed-ingest commands"""
    pass


@cli.command()
def version():
    """Get the library version."""
    click.echo(click.style("0.1.0", bold=True))


if __name__ == "__main__":
    dotenv.load_dotenv()
    cli()
