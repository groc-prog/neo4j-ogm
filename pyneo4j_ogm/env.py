"""
Environment variables used across the package and helper functions.
"""

from enum import Enum
from os import environ
from typing import Any


class EnvVariable(Enum):
    """
    Available environment variables for defining package behavior.
    """

    LOGGING_ENABLED = "PYNEO4J_LOGGING_ENABLED"
    LOGLEVEL = "PYNEO4J_LOGLEVEL"


def from_env(variable: EnvVariable, default: Any = None) -> Any:
    """
    Utility function for getting environment variables without having to deal with type
    casting each time.

    Args:
        variable (EnvVariable): The variable to get.
        default (Any): Default value to return if `variable` is not set. Defaults to
            `None`.

    Returns:
        Any: The unparsed value from the env variable.
    """

    return environ.get(variable.value, default)
