from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Tuple, TypedDict, Union


# TODO: Add missing actions here
class ActionType(Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    REFRESH = "REFRESH"


class ActionContext(TypedDict, total=False):
    type_: ActionType


ActionFunction = Callable[[ActionContext, Tuple[Any], Dict[Any, Any]], Union[None, Awaitable[None]]]


class EagerFetchStrategy(Enum):
    """
    Available strategies used when eagerly fetching neighboring nodes.
    """

    COMBINED = 0
    AS_SPLIT_QUERY = 1


class Cardinality(Enum):
    """
    Available cardinalities used to enforce a given number of relationships for a node.
    """

    NONE = "NONE"
    ZERO_OR_ONE = "ZERO_OR_ONE"
    ZERO_OR_MORE = "ZERO_OR_MORE"
    ONE = "ONE"
    ONE_OR_MORE = "ONE_OR_MORE"
    MULTIPLE = "MULTIPLE"
