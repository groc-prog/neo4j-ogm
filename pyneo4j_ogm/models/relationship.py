from typing import ClassVar, Generic, Optional, Self, TypeVar, cast

import neo4j.graph
from pydantic import PrivateAttr, computed_field

from pyneo4j_ogm.hash import generate_model_hash
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.models.decorators import (
    ensure_hydrated,
    ensure_not_destroyed,
    wrap_with_actions,
)
from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    RelationshipConfig,
    ValidatedRelationshipConfiguration,
)
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.model import ActionType

T = TypeVar("T", bound=Node)
U = TypeVar("U", bound=Node)


class Relationship(ModelBase, Generic[T, U]):
    """
    Base model for all relationship models. Every relationship model should inherit from this class to have needed base
    functionality like de-/inflation and validation.
    """

    ogm_config: ClassVar[RelationshipConfig]
    """
    Configuration for the pyneo4j model, should be a dictionary conforming to [`RelationshipConfig`][pyneo4j_ogm.options.model_options.RelationshipConfig].
    """

    _ogm_config: ClassVar[ValidatedRelationshipConfiguration] = PrivateAttr()
    _start_node: Optional[T] = None
    _end_node: Optional[U] = None

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(RelationshipConfig, getattr(super(cls, cls), "ogm_config"))

        # It does not make sense to merge relationship types when inheriting something so
        # we just always use the name if not stated otherwise
        if ("type" in parent_config and parent_config["type"] == model_config.type) or len(model_config.type) == 0:
            cls.ogm_config["type"] = cls.__name__.upper()

        cls._ogm_config = ValidatedRelationshipConfiguration.model_validate(cls.ogm_config)
        cls._hash = generate_model_hash(cls._ogm_config.type)
        cls._excluded_from_inflate.update(["start_node", "end_node"])

    @computed_field
    @property
    def start_node(self) -> Optional[T]:
        """
        Start node model for this relationship.
        """
        return self._start_node

    @computed_field
    @property
    def end_node(self) -> Optional[U]:
        """
        End node model for this relationship.
        """
        return self._end_node

    @ensure_hydrated
    @ensure_not_destroyed
    @wrap_with_actions(ActionType.REFRESH)
    async def refresh(self, fetch_nodes: bool = False) -> None:
        """
        Refreshes the instance with values from the database. Optionally, start- and
        end node can be fetched as well.

        This will overwrite any modified fields with the values from the database.

        Args:
            fetch_nodes (bool): Whether to fetch the start- and end node as well. Defaults
                to `False`.
        """
        logger.info(
            "Refreshing node %s for model %s with values from database", self._element_id, self.__class__.__name__
        )

        returned_entities = ["r, n, m"] if fetch_nodes else ["r"]
        match_pattern = QueryBuilder.build_relationship_pattern(
            "r", self._ogm_config.type, start_node_ref="n", end_node_ref="m"
        )
        element_id_predicate, element_id_parameters = QueryBuilder.build_element_id_predicate("r", self._element_id)

        result, _ = await self._registry.active_client.cypher(
            f"MATCH {match_pattern} WHERE {element_id_predicate} RETURN {', '.join(returned_entities)}",
            element_id_parameters,
        )

        logger.debug("Updating instance with entity values")
        self._element_id = cast(Self, result[0][0]).element_id
        self._id = cast(Self, result[0][0]).id
        self._graph = cast(Self, result[0][0]).graph
        self._state_snapshot = self.model_copy()

        for field_name in self.__class__.model_fields.keys():
            setattr(self, field_name, getattr(cast(Self, result[0][0]), field_name))

        if fetch_nodes:
            logger.debug("Updating start- and end nodes for instance")
            self._start_node = cast(T, result[0][1])
            self._end_node = cast(U, result[0][2])
        else:
            logger.debug("Resetting start- and end nodes for instance")
            self._start_node = None
            self._end_node = None

        self._state_snapshot = self.model_copy()
        logger.info("Refreshed node %s for model %s", self._element_id, self.__class__.__name__)

    @classmethod
    def _inflate(
        cls, graph_entity: neo4j.graph.Relationship, start_node: Optional[T] = None, end_node: Optional[U] = None
    ) -> Self:
        """
        Inflates a graph entity from the Neo4j driver into the current model.

        Args:
            graph_entity (neo4j.graph.Relationship): The graph relationship to inflate.
            start_node (Optional[Node]): The inflated node model. This should be the start node returned with
                the relationship from the DB. Defaults to `None`.
            end_node (Optional[Node]): The inflated node model. This should be the end node returned with
                the relationship from the DB. Defaults to `None`.

        Returns:
            Self: A inflated instance of the current model.
        """
        inflated = super()._inflate(graph_entity)

        if start_node is not None:
            inflated._start_node = start_node
        if end_node is not None:
            inflated._end_node = end_node

        return inflated
