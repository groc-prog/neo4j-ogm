from typing import Generic, List, Optional, Type, TypeVar

from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from typing_extensions import get_args

from pyneo4j_ogm.exceptions import MissingRelationshipPropertyTypes
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
        direction (RelationshipDirection): The direction used for the relationship. Defaults to
            `RelationshipDirection.BOTH`.
        cardinality (Cardinality): The cardinality between the nodes. This is only enforced
            by the client, not the database itself. Defaults to `Cardinality.NONE`.
    """

    _field_name: Optional[str]
    _owner_node: Optional[Type[Node]]
    _target_node: Optional[Type[T]]
    _relationship: Optional[Type[U]]
    _direction: RelationshipDirection
    _cardinality: Cardinality

    nodes: List[T]

    def __init__(
        self,
        direction: RelationshipDirection = RelationshipDirection.BOTH,
        cardinality: Cardinality = Cardinality.NONE,
    ) -> None:
        self._owner_node = None
        self._target_node = None
        self._relationship = None
        self._direction = direction
        self._cardinality = cardinality
        self.nodes = []

    def _initialize(self, owner: Type[Node], field_name: str) -> None:
        """
        Initializes the property by extracting the node/relationship class from the generics.

        Args:
            owner (Type[Node]): The node on which the relationship property is defined.
            field_name (str): The field name with which the relationship property has been defined.

        Raises:
            MissingRelationshipPropertyTypes: If no generics have been defined.
        """
        self._owner_node = owner
        self._field_name = field_name

        # Extract the node and relationship class types from the generics
        generic_types = getattr(self, "__orig_class__", None)
        if generic_types is None:
            raise MissingRelationshipPropertyTypes(self._owner_node.__name__, self._field_name)

        target_node, relationship = get_args(generic_types)
        self._target_node = target_node
        self._relationship = relationship

    def _get_cardinality_definition(self) -> CardinalityDefinitions:
        """
        Returns the cardinality definitions for this relationship property.

        Returns:
            CardinalityDefinitions: The identifying keys and their definitions.
        """
        if self._owner_node is None or self._target_node is None or self._relationship is None:
            raise ValueError("Relationship property not initialized")

        if self._direction != RelationshipDirection.BOTH:
            start = self._owner_node if self._direction == RelationshipDirection.OUTGOING else self._target_node
            end = self._target_node if self._direction == RelationshipDirection.OUTGOING else self._owner_node

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
                f"{self._owner_node._hash}__{self._relationship._hash}__{self._target_node._hash}__{self._cardinality.value}",
                {
                    "start": self._owner_node,
                    "end": self._target_node,
                    "relationship": self._relationship,
                    "type_": self._cardinality,
                },
            )
        )
        definitions.append(
            (
                f"{self._target_node._hash}__{self._relationship._hash}__{self._owner_node._hash}__{self._cardinality.value}",
                {
                    "start": self._target_node,
                    "end": self._owner_node,
                    "relationship": self._relationship,
                    "type_": self._cardinality,
                },
            )
        )

        return definitions
