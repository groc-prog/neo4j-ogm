from enum import Enum
from typing import Literal, Type, TypedDict, Union

from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship

# TODO: Add missing hooks here
Hooks = Union[
    Literal["create"],
    Literal["update"],
    Literal["delete"],
    Literal["refresh"],
    str,
]


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


class CardinalityDefinition(TypedDict):
    start: Type[Node]
    end: Type[Node]
    relationship: Type[Relationship]
    type_: Cardinality
