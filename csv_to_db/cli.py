import typing

import click

from .db import extract_file
from .files import csv_files, download_file_to_temp_file, download_git_repo, find_files_in_dir, remove_dir

base_options = [
    click.option("--db", "-o", help="The output database path",
                 type=click.Path(),
                 default="data.db"),
    click.option('--typing', "-t",
                 type=click.Choice(['full', 'quick', 'none']),
                 help="""Determines whether the script should guess the column type (int/float/string supported).
              quick: only base the types on the first line
full: read the entire file
none: no typing, every column is string""",
                 default='quick'),
    click.option("--drop-tables/--no-drop-tables", "-D",
                 help="Determines whether the tables should be dropped before creation, if they already exist"
                      " (BEWARE OF DATA LOSS)",
                 default=False),
    click.option("--headers/--no-headers", "-H",
                 help="Whether the CSV file(s) have headers",
                 default=True),
    click.option("--verbose", "-v",
                 is_flag=True,
                 help="Determines whether progress reporting messages should be printed",
                 default=False),
    click.option("--delimiter", "-x",
                 help="Choose the CSV delimiter. Defaults to comma. Hint: for tabs, in Bash use $'\\t'.",
                 default=","),
    click.option("--encoding", "-e",
                 help="Choose the input CSV's file encoding. Use the string identifier Python uses to specify encodings, e.g. 'windows-1250'.",
                 default="utf8")
]


def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


@click.group()
def cli():
    """
    Extract Venmo transactions from a profile with one command
    """
    pass


@cli.command()
@click.argument('filepaths', nargs=-1, type=click.Path(exists=True))
@add_options(base_options)
def file(filepaths: typing.List[str], **kwargs):
    """
    Convert space-separated list of CSV files into database
    """
    for filepath in filepaths:
        extract_file(kwargs.get('db'), filepath, headers=kwargs.get('headers'))
        # try:
        #     fh = is_file(filepath)
        # except Exception:
        #     click.echo(click.style(f"Found {len(files)} in specified directories", fg='red', bold=True))
        #     return 1
        # try:
        #     csv_file = is_csv_file(fh)
        # except Exception:
        #     click.echo(click.style(f"Found {len(files)} in specified directories", fg='red', bold=True))
        #     return 1


@cli.command()
@click.argument('dirpaths', nargs=-1, type=click.Path(exists=True))
@add_options(base_options)
def dir(dirpaths: typing.List[str], **kwargs):
    """
    Convert space-separated directories of CSV files into database
    """
    files = []
    for dirpath in dirpaths:
        files.extend(csv_files(dirpath))
    click.echo(click.style(f"Found {len(files)} in specified directories", fg='green', bold=True))

    for filepath in files:
        extract_file(kwargs.get('db'), filepath, headers=kwargs.get('headers'))


@cli.command()
@click.argument('url', nargs=1)
@add_options(base_options)
def url(url: str, **kwargs):
    """
    Retrieve remote CSV file from URL and input into database
    """
    # csv
    # zip
    try:
        filepath, filename = download_file_to_temp_file(url)
    except Exception as error:
        click.echo(error)
        click.echo(click.style(f"Unable to retrieve file at {url}", fg='red', bold=True))
        return 1

    table_name = filename.rsplit('.', 1)[0]
    extract_file(kwargs.get('db'), filepath, table_name=table_name, headers=kwargs.get('headers'), temp=True)
    # is_csv_file(file)
    # click.echo(url)
    # print(kwargs)


@cli.command()
@click.argument('repo', nargs=1)
@add_options(base_options)
def git(repo: str, **kwargs):
    """
    Retrieve remote git repository and input into database
    """

    path = download_git_repo(repo)
    files = find_files_in_dir(path, ['*.csv', '*.py', '*.txt'])
    remove_dir(path)

    # try:
    #     filepath, filename = download_file_to_temp_file(url)
    # except Exception as error:
    #     click.echo(error)
    #     click.echo(click.style(f"Unable to retrieve file at {url}", fg='red', bold=True))
    #     return 1
    #
    # extract_file(kwargs.get('db'), filepath, table_name=filename, headers=kwargs.get('headers'), temp=True)
    # is_csv_file(file)
    # click.echo(url)
    # print(kwargs)


# @cli.command()
# def watch():
#     """
#     Watch
#     """
#     make_signal_handlers()
    # observer = make_observer()
    # observer.start()
    # click.echo("Watching all files in specified directory")
    # try:
    #     while True:
    #         time.sleep(1)
    # except ProgramKilled:
    #     click.echo("Program killed: shutting down watcher and running cleanup code")
    #     observer.stop()
    #     observer.join()