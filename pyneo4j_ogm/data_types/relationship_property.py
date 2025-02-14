from typing import Generic, Type, TypeVar

from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship
from pyneo4j_ogm.types.client import CardinalityDefinition, CardinalityDefinitions
from pyneo4j_ogm.types.graph import RelationshipDirection
from pyneo4j_ogm.types.model import Cardinality

T = TypeVar("T", bound=Node)
U = TypeVar("U", bound=Relationship)


class RelationshipProperty(Generic[T, U]):
    """
    Class used for defining a relationship between two Node models. Can be configured
    to allow only certain cardinalities and directions.

    Args:
        node (Type[T]): The node model the relationship is defined to.
        relationship (Type[U]): The relationship model used.
        direction (RelationshipDirection): The direction used for the relationship.
        cardinality (Cardinality): The cardinality between the nodes. This is only enforced
            by the client, not the database itself. Defaults to `Cardinality.NONE`.
    """

    _node: Type[T]
    _relationship: Type[U]
    _direction: RelationshipDirection
    _cardinality: Cardinality

    def __init__(
        self,
        node: Type[T],
        relationship: Type[U],
        direction: RelationshipDirection,
        cardinality: Cardinality = Cardinality.NONE,
    ) -> None:
        self._node = node
        self._relationship = relationship
        self._direction = direction
        self._cardinality = cardinality

    def _get_cardinality_definition(self, owner: Type[Node]) -> CardinalityDefinitions:
        """
        Returns the cardinality definitions for this relationship property.

        Returns:
            CardinalityDefinitions: The identifying keys and their definitions.
        """
        if self._direction != RelationshipDirection.BOTH:
            start = owner if self._direction == RelationshipDirection.OUTGOING else self._node
            end = self._node if self._direction == RelationshipDirection.OUTGOING else owner

            identifier = f"{start._hash}__{self._relationship._hash}__{end._hash}__{self._cardinality.value}"
            definition: CardinalityDefinition = {
                "start": start,
                "end": end,
                "relationship": self._relationship,
                "type_": self._cardinality,
            }

            return [(identifier, definition)]

        # If the direction is defined as RelationshipDirection.BOTH, we will have two definitions
        # since the cardinality needs to apply in both directions
        definitions: CardinalityDefinitions = []
        definitions.append(
            (
                f"{owner._hash}__{self._relationship._hash}__{self._node._hash}__{self._cardinality.value}",
                {"start": owner, "end": self._node, "relationship": self._relationship, "type_": self._cardinality},
            )
        )
        definitions.append(
            (
                f"{self._node._hash}__{self._relationship._hash}__{owner._hash}__{self._cardinality.value}",
                {"start": self._node, "end": owner, "relationship": self._relationship, "type_": self._cardinality},
            )
        )

        return definitions
