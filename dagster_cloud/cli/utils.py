import os

import click


def add_options(options):
    """
    Share options between commands
    https://stackoverflow.com/questions/40182157/shared-options-and-flags-between-commands
    """

    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


DEFAULT_DAGIT_URL = lambda: os.getenv(
    "DAGSTER_CLOUD_DAGIT_URL",
    "http://localhost:3000",
)


def _api_token():
    return os.getenv("DAGSTER_CLOUD_API_TOKEN")


def _show_api_token_prompt():
    password_exists = bool(_api_token())
    show_prompt = not password_exists
    return show_prompt


create_cloud_dagit_client_options = [
    click.option("--url", type=click.STRING, default=DEFAULT_DAGIT_URL, help="Dagit url"),
    click.option(
        "--api-token",
        type=click.STRING,
        default=_api_token,
        prompt=_show_api_token_prompt(),
        hide_input=True,
        required=True,
        help="API token generated in Dagit",
    ),
]
