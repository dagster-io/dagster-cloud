import collections
import functools
import inspect
from typing import Any, Dict, List, Tuple

from typer.models import OptionInfo


def with_added_params(
    signature: inspect.Signature, to_add: Dict[str, inspect.Parameter]
) -> inspect.Signature:
    """
    Returns a new signature based on the provided one, with the provided parameters added.
    """
    params = collections.OrderedDict(signature.parameters)
    for k, v in to_add.items():
        params[k] = v
    return signature.replace(parameters=list(params.values()))


def without_params(signature: inspect.Signature, to_remove: List[str]) -> inspect.Signature:
    """
    Returns a new signature based on the provided one with the given parameters removed by name.
    Does nothing if the parameter is not found.
    """
    params = collections.OrderedDict(signature.parameters)
    for r in to_remove:
        if r in params:
            del params[r]
    return signature.replace(parameters=list(params.values()))


def add_options(options: Dict[str, Tuple[Any, OptionInfo]]):
    """Decorator to add Options to a particular command."""

    def decorator(to_wrap):
        # Ensure that the function signature has the correct typer.Option defaults,
        # so that every command need not specify them

        # Remove kwargs, since Typer doesn't know how to handle it
        to_wrap_sig = inspect.signature(to_wrap)
        has_kwargs = "kwargs" in to_wrap_sig.parameters
        sig = without_params(to_wrap_sig, ["kwargs"])
        sig = with_added_params(
            sig,
            {
                name: inspect.Parameter(
                    name,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=_type,
                    default=default,
                )
                for name, (_type, default) in options.items()
            },
        )

        @functools.wraps(to_wrap)
        def wrap_function(*args, **kwargs):
            modified_kwargs = kwargs
            if not has_kwargs and not hasattr(to_wrap, "modified_options"):
                modified_kwargs = {k: v for k, v in kwargs.items() if k in to_wrap_sig.parameters}
            return to_wrap(*args, **modified_kwargs)

        wrap_function.__signature__ = sig
        wrap_function.modified_options = True
        return wrap_function

    return decorator
