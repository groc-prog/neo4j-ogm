from typing import ClassVar, List, Set, cast

import neo4j.graph
from pydantic import PrivateAttr

from pyneo4j_ogm.exceptions import EntityAlreadyCreatedError, EntityNotFoundError
from pyneo4j_ogm.hash import generate_model_hash
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.models.decorators import (
    ensure_hydrated,
    ensure_not_destroyed,
    wrap_with_actions,
)
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    NodeConfig,
    ValidatedNodeConfiguration,
)
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.model import ActionType


class Node(ModelBase):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.
    """

    ogm_config: ClassVar[NodeConfig]
    """
    Configuration for the pyneo4j model, should be a dictionary conforming to [`NodeConfig`][pyneo4j_ogm.options.model_options.NodeConfig].
    """

    _ogm_config: ClassVar[ValidatedNodeConfiguration] = PrivateAttr()

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(NodeConfig, getattr(super(cls, cls), "ogm_config"))
        parent_labels = cast(Set[str], parent_config["labels"]) if "labels" in parent_config else set()
        custom_labels = set(model_config.labels).difference(parent_labels)

        if "labels" not in cls.ogm_config:
            # We should never reach this line since the config should always be set in
            # the base class when initializing
            raise ValueError("'labels' property not initialized")  # pragma: no cover

        if len(custom_labels) == 0:
            cast(List[str], cls.ogm_config["labels"]).append(cls.__name__)

        cls._ogm_config = ValidatedNodeConfiguration.model_validate(cls.ogm_config)
        cls._hash = generate_model_hash(cls._ogm_config.labels)

    async def create(self):
        """
        Inserts the current instance into the database by creating a new graph entity from it. The
        current instance will then be updated with the `id` and `element_id` from the database.

        After the method is finished, a newly created instance is seen as `hydrated` and all methods
        can be called on it.

        Raises:
            EntityAlreadyCreatedError: If the instance has already been created and hydrated.
        """
        if self.hydrated:
            logger.error("Node %s has already been created with ID %s", self.__class__.__name__, self.element_id)
            raise EntityAlreadyCreatedError(self.__class__.__name__, cast(str, self.element_id))

        logger.info("Creating new node %s", self.__class__.__name__)
        deflated = self._deflate(self.model_dump(exclude={"element_id", "id"}))

        match_pattern = QueryBuilder.build_node_pattern("n", self._ogm_config.labels)
        set_clause, parameters = QueryBuilder.build_set_clause("n", deflated)

        result, _ = await self._registry.active_client.cypher(
            f"CREATE {match_pattern} {set_clause} RETURN n", parameters
        )

        logger.debug("Hydrating instance with entity values")
        self._element_id = cast(neo4j.graph.Node, result[0][0]).element_id
        self._id = cast(neo4j.graph.Node, result[0][0]).id
        self._graph = cast(neo4j.graph.Node, result[0][0]).graph
        self._state_snapshot = self.model_copy()
        logger.info("Created new node %s for model %s", self._element_id, self.__class__.__name__)

    @ensure_hydrated
    @ensure_not_destroyed
    async def update(self):
        update_count = len(self.modified_fields)
        logger.info("Updating %s properties on node %s", update_count, self._element_id)
        deflated = self._deflate(self.model_dump(include=self.modified_fields, exclude={"element_id", "id"}))

        match_pattern = QueryBuilder.build_node_pattern("n", self._ogm_config.labels)
        set_clause, parameters = QueryBuilder.build_set_clause("n", deflated)

        result, _ = await self._registry.active_client.cypher(
            f"MATCH {match_pattern} {set_clause} RETURN n", parameters
        )

        if len(result) == 0:
            raise EntityNotFoundError(EntityType.NODE, self._ogm_config.labels, cast(str, self._element_id))

        logger.debug("Resetting tracked model state")
        self._state_snapshot = self.model_copy()
        logger.info("Updated %d properties on model %s", update_count, self._element_id)
