import collections
import functools
import inspect
import os
from typing import List, NamedTuple, Optional, cast

import yaml
from click import Context
from dagster_cloud.errors import DagsterCloudHTTPError
from typer import Option

from . import gql, ui
from .utils import add_options

DEPLOYMENT_CLI_ARGUMENT = "deployment"
DEPLOYMENT_ENV_VAR_NAME = "DAGSTER_CLOUD_DEPLOYMENT"

ORGANIZATION_CLI_ARGUMENT = "organization"
ORGANIZATION_ENV_VAR_NAME = "DAGSTER_CLOUD_ORGANIZATION"

TOKEN_CLI_ARGUMENT = "api-token"
TOKEN_CLI_ARGUMENT_VAR = TOKEN_CLI_ARGUMENT.replace("-", "_")
TOKEN_ENV_VAR_NAME = "DAGSTER_CLOUD_API_TOKEN"

URL_CLI_ARGUMENT = "url"
URL_ENV_VAR_NAME = "DAGSTER_CLOUD_DAGIT_URL"

DEFAULT_CLOUD_CLI_FOLDER = os.path.join(os.path.expanduser("~"), ".dagster_cloud_cli")
DEFAULT_CLOUD_CLI_CONFIG = os.path.join(DEFAULT_CLOUD_CLI_FOLDER, "config")

AGENT_TIMEOUT_CLI_ARGUMENT = "agent-timeout"
AGENT_TIMEOUT_ARGUMENT_VAR = AGENT_TIMEOUT_CLI_ARGUMENT.replace("-", "_")
AGENT_TIMEOUT_ENV_VAR_NAME = "DAGSTER_CLOUD_AGENT_TIMEOUT"
DEFAULT_AGENT_TIMEOUT = 300


class DagsterCloudCliConfig(
    NamedTuple(
        "_DagsterCloudCliConfig",
        [
            ("organization", Optional[str]),
            ("default_deployment", Optional[str]),
            ("user_token", Optional[str]),
            ("agent_timeout", Optional[int]),
        ],
    )
):
    __slots__ = ()

    def __new__(cls, **kwargs):
        none_defaults = {k: (kwargs[k] if k in kwargs else None) for k in cls._fields}
        return super(DagsterCloudCliConfig, cls).__new__(cls, **none_defaults)


def get_config_path():
    return os.getenv("DAGSTER_CLOUD_CLI_CONFIG", DEFAULT_CLOUD_CLI_CONFIG)


def write_config(config: DagsterCloudCliConfig):
    """
    Writes the given config object to the CLI config file.
    """
    config_path = get_config_path()
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, "w", encoding="utf8") as f:
        config_dict = {k: v for k, v in config._asdict().items() if v is not None}
        f.write(yaml.dump(config_dict))


def read_config() -> DagsterCloudCliConfig:
    """
    Reads the CLI config file into a config object.
    """
    config_path = get_config_path()
    if not os.path.isfile(config_path):
        return DagsterCloudCliConfig()

    with open(config_path, "r", encoding="utf8") as f:
        raw_in = yaml.load(f.read(), Loader=yaml.SafeLoader)
        return DagsterCloudCliConfig(**raw_in)


def get_deployment(ctx: Optional[Context] = None) -> Optional[str]:
    """
    Gets the configured deployment to target.
    Highest precedence is a deployment argument, then `DAGSTER_CLOUD_DEPLOYMENT`
    env var, then `~/.dagster_cloud_cli/config` default.
    """
    if ctx and ctx.params.get(DEPLOYMENT_CLI_ARGUMENT):
        return ctx.params[DEPLOYMENT_CLI_ARGUMENT]
    return os.getenv(DEPLOYMENT_ENV_VAR_NAME, read_config().default_deployment)


def get_organization(ctx: Optional[Context] = None) -> Optional[str]:
    """
    Gets the configured organization to target.
    Highest precedence is an organization argument, then `DAGSTER_CLOUD_ORGANIZATION`
    env var, then `~/.dagster_cloud_cli/config` value.
    """
    if ctx and ctx.params.get(ORGANIZATION_CLI_ARGUMENT):
        return ctx.params[ORGANIZATION_CLI_ARGUMENT]
    return os.getenv(ORGANIZATION_ENV_VAR_NAME, read_config().organization)


def get_agent_timeout() -> int:
    """
    Gets the configured agent timeout to target.
    Highest precedence is an agent-timeout argument, then `DAGTER_CLOUD_AGENT_TIMEOUT`
    env var, then `~/.dagster_cloud_cli/config` value.
    """
    config_timeout = read_config().agent_timeout
    default_timeout = config_timeout if config_timeout != None else DEFAULT_AGENT_TIMEOUT
    assert default_timeout is not None

    env_val = os.getenv(AGENT_TIMEOUT_ENV_VAR_NAME)

    return int(cast(str, env_val)) if env_val != None else default_timeout


def get_user_token(ctx: Optional[Context] = None) -> Optional[str]:
    """
    Gets the configured user token to use.
    Highest precedence is an api-token argument, then `DAGSTER_CLOUD_API_TOKEN`
    env var, then `~/.dagster_cloud_cli/config` value.
    """
    if ctx and ctx.params.get(TOKEN_CLI_ARGUMENT_VAR):
        return ctx.params[TOKEN_CLI_ARGUMENT_VAR]
    return os.getenv(TOKEN_ENV_VAR_NAME, read_config().user_token)


def available_deployment_names(ctx, incomplete: str = "") -> List[str]:
    """
    Gets a list of deployment names given the Typer Context, used for CLI completion.
    """
    organization = get_organization(ctx=ctx)
    user_token = get_user_token(ctx=ctx)

    if not organization or not user_token:
        return []
    try:
        deployments = gql.fetch_deployments(
            gql.graphql_client_from_url(gql.url_from_config(organization), user_token)
        )
        names = [deployment["deploymentName"] for deployment in deployments]
        return [name for name in names if name.startswith(incomplete)]
    except DagsterCloudHTTPError:
        return []


def get_url(ctx: Optional[Context] = None) -> Optional[str]:
    """
    Gets the url passed in or from the environment.
    """
    if ctx and ctx.params.get(URL_CLI_ARGUMENT):
        return ctx.params[URL_CLI_ARGUMENT]
    return os.getenv(URL_ENV_VAR_NAME)


# Typer Option definitions for common CLI config options (organization, deployment, user token)
ORGANIZATION_OPTION = Option(
    get_organization,
    "--organization",
    "-o",
    help="Organization to target.",
    show_default=get_organization(),  # type: ignore
)
DEPLOYMENT_OPTION = Option(
    get_deployment,
    "--deployment",
    "-d",
    help="Deployment to target.",
    autocompletion=available_deployment_names,
    show_default=get_deployment(),  # type: ignore
)
USER_TOKEN_OPTION = Option(
    get_user_token,
    "--api-token",
    "--user-token",
    "-u",
    help="Cloud user token.",
    show_default=ui.censor_token(get_user_token()) if get_user_token() else None,  # type: ignore
)
URL_OPTION = Option(
    get_url,
    "--url",
    help="[DEPRECATED] Your Dagster Cloud url, in the form of 'https://{ORGANIZATION_NAME}.dagster.cloud/{DEPLOYMENT_NAME}'.",
    hidden=True,
)

AGENT_TIMEOUT_OPTION = Option(
    get_agent_timeout(),
    "--agent-timeout",
    help="After making changes to the workspace. how long in seconds to wait for the agent before timing out with an error",
)


def dagster_cloud_options(
    allow_empty: bool = False,
    allow_empty_deployment: bool = False,
    requires_url: bool = False,
):
    """
    Apply this decorator to Typer commands to make them take the
    `organization`, `deployment`, and `api-token` arguments.
    These values are passed as keyword arguments.

    Unless `allow_empty` or `allow_empty_deployment` are set, an error
    will be raised if these values are not specified as arguments or via config/env var.

    Set `requires_url` if the command needs a Dagit url; if so, this will be provided
    via the `url` keyword argument.
    """

    def decorator(to_wrap):
        wrapped_sig = inspect.signature(to_wrap)
        params = collections.OrderedDict(wrapped_sig.parameters)

        options = {
            ORGANIZATION_CLI_ARGUMENT: (str, ORGANIZATION_OPTION),
            TOKEN_CLI_ARGUMENT_VAR: (str, USER_TOKEN_OPTION),
        }

        # For requests that need a graphql client, a user can supply a URL (though this
        # is a hidden option), or an organization and deployment
        if requires_url:
            options[URL_CLI_ARGUMENT] = (str, URL_OPTION)

        has_deployment_param = (
            DEPLOYMENT_CLI_ARGUMENT in params
            and params[DEPLOYMENT_CLI_ARGUMENT].default is params[DEPLOYMENT_CLI_ARGUMENT].empty
        ) or requires_url
        if has_deployment_param:
            options[DEPLOYMENT_CLI_ARGUMENT] = (str, DEPLOYMENT_OPTION)

        has_agent_timeout_param = AGENT_TIMEOUT_ARGUMENT_VAR in params
        if has_agent_timeout_param:
            options[AGENT_TIMEOUT_ARGUMENT_VAR] = (int, AGENT_TIMEOUT_OPTION)

        with_options = add_options(options)(to_wrap)

        @functools.wraps(with_options)
        def wrap_function(*args, **kwargs):

            # If underlying command needs a URL and none is explicitly provided,
            # generate one from the organization + deployment
            if (
                requires_url
                and not kwargs.get(URL_CLI_ARGUMENT)
                and kwargs.get(ORGANIZATION_CLI_ARGUMENT)
                and kwargs.get(DEPLOYMENT_CLI_ARGUMENT)
            ):
                kwargs[URL_CLI_ARGUMENT] = gql.url_from_config(
                    organization=kwargs.get(ORGANIZATION_CLI_ARGUMENT),
                    deployment=kwargs.get(DEPLOYMENT_CLI_ARGUMENT),
                )

            lacking_url = requires_url and not kwargs.get(URL_CLI_ARGUMENT)

            # Raise errors if important options are not provided
            if not kwargs.get(TOKEN_CLI_ARGUMENT_VAR) and (not allow_empty or lacking_url):
                raise ui.error(
                    f"A Dagster Cloud user token must be specified for this command.\n\nYou may specify a token by:\n"
                    f"- Providing the {ui.as_code('--' + TOKEN_CLI_ARGUMENT)} parameter.\n"
                    f"- Setting the {ui.as_code(TOKEN_ENV_VAR_NAME)} environment variable.\n"
                    f"- Specifying {ui.as_code('user_token')} in your config file ({get_config_path()}), run {ui.as_code('dagster-cloud config setup')}."
                )
            if not kwargs.get(ORGANIZATION_CLI_ARGUMENT) and (not allow_empty or lacking_url):
                raise ui.error(
                    f"A Dagster Cloud organization must be specified for this command.\n\nYou may specify your organization by:\n"
                    f"- Providing the {ui.as_code('--' + ORGANIZATION_CLI_ARGUMENT)} parameter.\n"
                    f"- Setting the {ui.as_code(ORGANIZATION_ENV_VAR_NAME)} environment variable.\n"
                    f"- Specifying {ui.as_code('organization')} in your config file ({get_config_path()}), run {ui.as_code('dagster-cloud config setup')}."
                )
            if (
                DEPLOYMENT_CLI_ARGUMENT in kwargs
                and not kwargs.get(DEPLOYMENT_CLI_ARGUMENT)
                and (not (allow_empty or allow_empty_deployment) or lacking_url)
            ):
                raise ui.error(
                    f"A Dagster Cloud deployment must be specified for this command.\n\nYou may specify a deployment by:\n"
                    f"- Providing the {ui.as_code('--' + DEPLOYMENT_CLI_ARGUMENT)} parameter.\n"
                    f"- Setting the {ui.as_code(DEPLOYMENT_ENV_VAR_NAME)} environment variable.\n"
                    f"- Running {ui.as_code('dagster-cloud config set-deployment <deployment_name>')}.\n"
                    f"- Specifying {ui.as_code('default_deployment')} in your config file ({get_config_path()})."
                )

            new_kwargs = dict(kwargs)
            with_options(*args, **new_kwargs)

        return wrap_function

    return decorator
