import datetime
import json
from typing import Any, Callable, List, Optional, Sequence, Union

import questionary
import typer
import yaml


class ExitWithMessage(typer.Exit):
    def __init__(self, message=None, *args, **kwargs):
        self.message = message
        super().__init__(*args, **kwargs)


def as_code(text: str) -> str:
    """Returns the given text colored and in backticks."""
    return typer.style(f"`{text}`", fg=typer.colors.CYAN)


def red(text: str) -> str:
    return typer.style(text, fg=typer.colors.RED)


def blue(text: str) -> str:
    return typer.style(text, fg=typer.colors.BLUE)


def yellow(text: str) -> str:
    return typer.style(text, fg=typer.colors.YELLOW)


def green(text: str) -> str:
    return typer.style(text, fg=typer.colors.GREEN)


def warn(message: str) -> None:
    """Prints a warning message."""
    print(f"{yellow('Warning:')} {message}")


def error(message: str, code: int = 1) -> Exception:
    """Prints an error message and returns an exception."""
    print(f"{red('Error:')} {message}")
    return ExitWithMessage(message=message, code=code)


def success(message: str) -> None:
    """Prints a success message."""
    print(f"{green('Success:')} {message}")


def censor_token(token: str) -> str:
    return ("*" * (len(token) - 6)) + token[-6:]


def list_input(
    prompt: str,
    choices: list[Any],
    default: Optional[Union[str, questionary.Choice, dict[str, Any]]] = None,
) -> str:
    """Presents the user with a list of choices that can be navigated with
    the up and down arrows.
    """
    return questionary.select(prompt, choices=choices, default=default).unsafe_ask()


def choice(value: Any, name: str) -> Any:
    return questionary.Choice(title=name, value=value)


def input(prompt: str, default: str = "", validate: Optional[Callable] = None) -> str:  # noqa: A001
    """Prompts the user for text input."""
    return questionary.text(prompt, default=default, validate=validate).unsafe_ask()


def password_input(prompt: str, default: str = "") -> str:
    """Prompts the user for text input, hidden by dots."""
    return questionary.password(prompt, default=default).unsafe_ask()


def print(*args, **kwargs):  # noqa: A001
    """Echos text to the console."""
    return typer.echo(*args, **kwargs)


def print_json(data: Any):
    return typer.echo(json.dumps(data, indent=2))


def print_yaml(data: Any):
    return typer.echo(yaml.dump(data))


def print_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    """Print a table with headers and rows."""
    if not rows:
        return
    
    # Calculate the maximum width for each column based on headers and rows
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            cell_str = str(cell)
            col_widths[i] = max(col_widths[i], len(cell_str))
    
    # Print the headers
    header_row = " │ ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(header_row)
    
    # Print the separator
    separator = "─┼─".join("─" * width for width in col_widths)
    print(separator)
    
    # Print each row
    for row in rows:
        row_str = " │ ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
        print(row_str)


def format_timestamp(timestamp: float) -> str:
    """Format a timestamp as a human-readable date string."""
    dt = datetime.datetime.fromtimestamp(timestamp)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def erase_previous_line(number_of_lines: int = 1) -> None:
    """Erases the previous line of output, which can then be printed over."""
    return typer.echo("\033[F\033[K" * number_of_lines + "\033[F")


def confirm(prompt: str, default: bool = True) -> bool:
    """Asks the user to respond yes or no to the prompt."""
    return questionary.confirm(prompt, default=default).unsafe_ask()


def bool_input(prompt: str, default: bool = False) -> bool:
    return confirm(prompt, default=default)
