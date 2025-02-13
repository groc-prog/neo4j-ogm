from typing import Type, TypedDict

from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship
from pyneo4j_ogm.types.model import Cardinality


class CardinalityDefinition(TypedDict):
    start: Type[Node]
    end: Type[Node]
    relationship: Type[Relationship]
    type_: Cardinality
