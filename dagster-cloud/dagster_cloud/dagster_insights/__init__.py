import sys
from typing import Any

from .dbt_wrapper import dbt_with_snowflake_insights as dbt_with_snowflake_insights
from .definitions import (
    create_snowflake_insights_asset_and_schedule as create_snowflake_insights_asset_and_schedule,
)
from .snowflake.snowflake_utils import (
    meter_snowflake_query as meter_snowflake_query,
)

dagster_snowflake_req_imports = {
    "InsightsSnowflakeResource",
}
try:
    from .snowflake.insights_snowflake_resource import (
        InsightsSnowflakeResource as InsightsSnowflakeResource,
    )


except ImportError:
    pass


# This is overriden in order to provide a better error message
# when the user tries to import a symbol which relies on another integration
# being installed.
def __getattr__(name) -> Any:
    obj = sys.modules[__name__].__dict__.get(name)
    if not obj:
        if name in dagster_snowflake_req_imports:
            raise ImportError(
                f"You are trying to import {name}, but the "
                "dagster-snowflake library is not installed. You can install it with "
                "`pip install dagster-snowflake`.",
            )
        else:
            raise AttributeError(f"module {__name__} has no attribute {name}")
    return obj
