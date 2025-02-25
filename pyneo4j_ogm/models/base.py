import json
from abc import abstractmethod
from copy import deepcopy
from typing import Any, ClassVar, Dict, List, Optional, Self, Set, Union, cast

import neo4j.graph
from pydantic import (
    BaseModel,
    PrivateAttr,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    model_serializer,
)
from typing_extensions import get_args, get_origin

from pyneo4j_ogm.data_types import ALLOWED_NEO4J_LIST_TYPES, ALLOWED_TYPES
from pyneo4j_ogm.exceptions import DeflationError, EntityNotFoundError, InflationError
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.models.decorators import (
    ensure_hydrated,
    ensure_not_destroyed,
    wrap_with_actions,
)
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    ValidatedNodeConfiguration,
    ValidatedRelationshipConfiguration,
)
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.registry import Registry
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.model import ActionType


class ModelBase(BaseModel):
    """
    Base class for all models types. Implements configuration merging and deflation/inflation
    of model properties.
    """

    _id: Optional[int] = PrivateAttr(None)
    _element_id: Optional[str] = PrivateAttr(None)
    _destroyed: bool = PrivateAttr(False)
    _graph: Optional[neo4j.graph.Graph] = PrivateAttr(None)
    _state_snapshot: Optional[Self] = PrivateAttr(None)

    _registry: Registry = PrivateAttr()
    _excluded_from_inflate: Set[str] = PrivateAttr()
    _ogm_config: ClassVar[Union[ValidatedNodeConfiguration, ValidatedRelationshipConfiguration]] = PrivateAttr()
    _hash: ClassVar[str]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry = Registry()
        cls._excluded_from_inflate = {"element_id", "id", "graph", "hydrated", "destroyed", "modified_fields"}

        parent_config = ModelConfigurationValidator(
            **getattr(
                super(cls, cls),
                "ogm_config",
                {},
            )
        )
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))

        merged_config = cls.__merge_config(parent_config.model_dump(), model_config.model_dump())
        setattr(cls, "ogm_config", ModelConfigurationValidator(**merged_config).model_dump())

    @model_serializer(mode="wrap")
    def serialize_model(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> Dict[str, Any]:
        serialized = handler(self)

        if (
            (info.exclude is None or "id" not in info.exclude)
            and (not info.exclude_none or (info.exclude_none and self._id is not None))
            and (info.include is None or "id" in info.include)
        ):
            serialized["id"] = self._id

        if (
            (info.exclude is None or "element_id" not in info.exclude)
            and (not info.exclude_none or (info.exclude_none and self._element_id is not None))
            and (info.include is None or "element_id" in info.include)
        ):
            serialized["element_id"] = self._element_id

        return serialized

    @ensure_hydrated
    @ensure_not_destroyed
    @wrap_with_actions(ActionType.UPDATE)
    async def update(self) -> None:
        """
        Updates the corresponding graph entity in the database and synchronizes it's properties
        with the current instance.

        Raises:
            EntityNotFoundError: If the entity is not found in the graph.
        """
        from pyneo4j_ogm.models.node import Node

        update_count = len(self.modified_fields)
        is_node = isinstance(self, Node)

        if update_count == 0:
            logger.info("No modified properties, skipping query")
            return

        logger.info(
            "Updating %s properties on %s %s", update_count, "node" if is_node else "relationship", self._element_id
        )
        deflated = self._deflate(self.model_dump(include=self.modified_fields, exclude=self._excluded_from_inflate))

        match_pattern: str
        set_clause, set_parameters = QueryBuilder.build_set_clause("e", deflated)
        element_id_predicate, element_id_parameters = QueryBuilder.build_element_id_predicate("e", self._element_id)

        if is_node:
            match_pattern = QueryBuilder.build_node_pattern("e", self._ogm_config.labels)
        else:
            match_pattern = QueryBuilder.build_relationship_pattern("e", self._ogm_config.type)

        result, _ = await self._registry.active_client.cypher(
            f"MATCH {match_pattern} WHERE {element_id_predicate} {set_clause} RETURN e",
            {**set_parameters, **element_id_parameters},
            resolve_models=False,
        )

        if len(result) == 0:
            raise EntityNotFoundError(
                EntityType.NODE if is_node else EntityType.RELATIONSHIP,
                self._ogm_config.labels if is_node else self._ogm_config.type,
                cast(str, self._element_id),
            )

        logger.debug("Resetting tracked model state")
        self._state_snapshot = self.model_copy()
        logger.info("Updated %d properties on model %s", update_count, self._element_id)

    @ensure_hydrated
    @ensure_not_destroyed
    @wrap_with_actions(ActionType.DELETE)
    async def delete(self) -> None:
        """
        Deletes the corresponding graph entity in the database.

        After the method is finished, the current instance is seen as `destroyed` and all methods
        called on it will raise `EntityDestroyedError`.
        """
        from pyneo4j_ogm.models.node import Node

        is_node = isinstance(self, Node)

        logger.info("Deleting %s %s", "node" if is_node else "relationship", self._element_id)

        match_pattern: str
        element_id_predicate, element_id_parameters = QueryBuilder.build_element_id_predicate("e", self._element_id)

        if is_node:
            match_pattern = QueryBuilder.build_node_pattern("e", self._ogm_config.labels)
        else:
            match_pattern = QueryBuilder.build_relationship_pattern("e", self._ogm_config.type)

        await self._registry.active_client.cypher(
            f"MATCH {match_pattern} WHERE {element_id_predicate} DETACH DELETE e",
            element_id_parameters,
            resolve_models=False,
        )

        logger.info("Deleted model %s", self._element_id)
        logger.debug("Marking instance as destroyed")
        self._destroyed = True
        self._element_id = None
        self._id = None

    # @abstractmethod
    # async def refresh(self) -> None:
    #     """
    #     Refreshes the instance with values from the database.
    #     """
    #     pass  # pragma: no cover

    @property
    def element_id(self) -> Optional[str]:
        return self._element_id

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def graph(self) -> Optional[neo4j.graph.Graph]:
        return self._graph

    @property
    def hydrated(self) -> bool:
        """
        Whether the property has been hydrated.
        """
        return self._id is not None and self._element_id is not None

    @property
    def destroyed(self) -> bool:
        """
        Whether the instance has been destroyed by calling `.destroy()` on it.
        """
        return self._destroyed

    @property
    def modified_fields(self) -> Set[str]:
        """
        Set of fields which have been updated since the last sync with the database.
        """
        snapshot = cast(Optional[Self], getattr(self, "_state_snapshot", None))
        if snapshot is None:
            return set()

        modified: Set[str] = set()
        for field_name in self.__class__.model_fields.keys():
            if getattr(self, field_name) != getattr(snapshot, field_name):
                modified.add(field_name)

        return modified

    @classmethod
    def _inflate(cls, graph_entity: Union[neo4j.graph.Node, neo4j.graph.Relationship]) -> Self:
        """
        Inflates a graph entity from the Neo4j driver into the current model.

        Args:
            graph_entity (Union[neo4j.graph.Node, neo4j.graph.Relationship]): The graph entity to inflate.

        Returns:
            Self: A inflated instance of the current model.
        """
        from pyneo4j_ogm.clients.neo4j import Neo4jClient

        client = cls._registry.active_client

        logger.debug("Inflating graph entity %s into model %s", graph_entity.element_id, cls.__name__)
        graph_properties = dict(graph_entity)

        inflatable: Dict[str, Any] = {}

        for field_name, field_info in cls.model_fields.items():
            if field_name not in graph_properties:
                # Fields with a value of `None` are omitted when writing to the DB, so we can assume this is
                # one of those cases. If not we will get a validation error either way.
                logger.debug(
                    "Graph entity %s missing property %s, using None as default", graph_entity.element_id, field_name
                )
                inflatable[field_name] = None
                continue

            logger.debug("Inflating property %s", field_name)
            graph_value = graph_properties[field_name]

            # Since Neo4j can have properties which have been stringified, we need to check if we
            # need to parse them before validating the model
            is_str = False

            if field_info.annotation is not None:
                origin = get_origin(field_info.annotation)

                if origin is None:
                    is_str = issubclass(field_info.annotation, str)
                elif origin is Union:
                    is_str = any(issubclass(arg, str) for arg in get_args(field_info.annotation))

            if isinstance(graph_value, str) and field_info.annotation is not None and not is_str:
                if isinstance(client, Neo4jClient) and getattr(client, "_allow_nested_properties", True) is False:
                    logger.error(
                        "Encountered stringified property %s, but `allow_nested_properties` is set to False", field_name
                    )
                    raise InflationError(cls.__name__)

                try:
                    logger.debug("Attempting to decode stringified property %s for model %s", field_name, cls.__name__)
                    inflatable[field_name] = json.loads(graph_value)
                except json.JSONDecodeError as exc:
                    logger.error("Stringified property %s of model %s can not be decoded", field_name, cls.__name__)
                    raise InflationError(cls.__name__) from exc
            else:
                inflatable[field_name] = graph_value

        inflated = cls.model_validate(inflatable)
        inflated._graph = graph_entity.graph
        inflated._element_id = graph_entity.element_id
        inflated._id = graph_entity.id

        # Reset modified fields
        setattr(inflated, "_state_snapshot", inflated.model_copy())

        return inflated

    @classmethod
    def _deflate(cls, dict_model: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deflates the provided model instance into a dictionary which can be stored by the Neo4j driver.

        Args:
            dict_model (Dict[str, Any]): The model to deflate as a dictionary.

        Returns:
            Dict[str, Any]: The storable dictionary.
        """
        from pyneo4j_ogm.clients.neo4j import Neo4jClient

        is_neo4j_client = isinstance(cls._registry.active_client, Neo4jClient)
        stringify_nested_properties = (
            False if not is_neo4j_client else getattr(cls._registry.active_client, "_allow_nested_properties", True)
        )

        logger.debug("Deflating model %s into storable format", cls.__name__)
        deflated = deepcopy(dict_model)

        for key, value in deflated.items():
            logger.debug("Validating property %s for model %s")
            deflated[key] = cls.__ensure_storable_value(value, is_neo4j_client, stringify_nested_properties)

        return deflated

    @classmethod
    def __ensure_storable_value(
        cls, value: Any, is_neo4j_client: bool, stringify_nested_properties: bool, depth: int = 0
    ) -> Any:
        """
        Validates that a given value is storable. Will parse and validate the value based on whether the client
        is for Neo4j or Memgraph. If a non-storable type is provided, it will be converted to a storable one, if
        possible.

        Args:
            value (Any): The value to validate and parse.
            is_neo4j_client (bool): Whether the currently active client is for Neo4j.
            stringify_nested_properties (bool): Whether to stringify nested properties or raise a exception. Only
                relevant for Neo4j clients.
            depth (int): The recursion depth. Should not be called with any other value than 0. Defaults to 0.

        Raises:
            DeflationError: If `is_neo4j_client` is true and nested properties are not allowed.
            DeflationError: If stringification of a property fails.
            DeflationError: If `is_neo4j_client` is true and a list property is not homogeneous.

        Returns:
            Any: The parsed and validated value.
        """
        storable = cls.__to_storable_value(value)

        if isinstance(storable, dict):
            if is_neo4j_client and depth == 0:
                # Since Neo4j can not store nested dictionaries, we can either serialized to a string before storage
                # or throw an error if this is not enabled
                if stringify_nested_properties:
                    try:
                        storable = json.dumps(storable)
                    except Exception as exc:
                        logger.error("Failed to stringify nested property: %s", exc)
                        raise DeflationError(cls.__name__) from exc
                else:
                    logger.error("Encountered nested property, but `allow_nested_properties` is set to False")
                    raise DeflationError(cls.__name__)
            else:
                for key, maybe_storable in storable.items():
                    # Recursively go through all nested properties
                    storable[key] = cls.__ensure_storable_value(
                        maybe_storable, is_neo4j_client, stringify_nested_properties, depth + 1
                    )
        elif isinstance(storable, (list, tuple)):
            is_tuple = False
            type_ = None

            # We need to convert any tuples since we might need to parse value inside of it. This way we can handle it the same
            # way we would handle lists
            if isinstance(storable, tuple):
                is_tuple = True
                storable = list(storable)

            for index, maybe_storable in enumerate(storable):
                # We need to convert it to a storable item here since we need to check for a homogeneous collection
                # when dealing with a Neo4j client
                storable_item = cls.__to_storable_value(maybe_storable)

                # Same as with handling dictionaries, we fall back to serializing the value to a string if Neo4j does not
                # support it as a collection item
                if is_neo4j_client and type(storable_item) not in ALLOWED_NEO4J_LIST_TYPES:
                    if stringify_nested_properties:
                        try:
                            storable_item = json.dumps(storable_item)
                        except Exception as exc:
                            logger.error("Failed to stringify nested property in model %s: %s", cls.__name__, exc)
                            raise DeflationError(cls.__name__) from exc
                    else:
                        logger.error(
                            "Encountered unsupported collection property in model %s, but `allow_nested_properties` is set to False",
                            cls.__name__,
                        )
                        raise DeflationError(cls.__name__)

                # Check whether the collection is homogeneous
                if is_neo4j_client and type_ is not None and type_ is not type(storable_item):
                    logger.error(
                        "Found non-homogeneous collection property in model %s while using Neo4j client", cls.__name__
                    )
                    raise DeflationError(cls.__name__)

                type_ = type(storable_item)
                cast(List, storable)[index] = cls.__ensure_storable_value(
                    storable_item, is_neo4j_client, stringify_nested_properties, depth + 1
                )

            if is_tuple:
                storable = tuple(storable)

        return storable

    @classmethod
    def __to_storable_value(cls, value: Any) -> Any:
        """
        Attempts to parse the given value into a comparable, storable type if not already
        storable.

        Args:
            value (Any): The type to convert to a storable type.

        Raises:
            DeflationError: If the type can not be converted to a storable type.

        Returns:
            Any: The corresponding storable type.
        """
        if type(value) in ALLOWED_TYPES:
            return value

        # TODO: Check what data types might be good to parse here for storage
        if isinstance(value, set):
            return list(value)

        logger.error("Non storable type %s could not be parsed", type(value))
        raise DeflationError(cls.__name__)

    @classmethod
    def __merge_config(cls, current_config: Dict[str, Any], updated_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merges configuration dictionaries.

        Args:
            current_config (Dict[str, Any]): The current configuration dictionary.
            updated_config (Dict[str, Any]): The configuration dictionary with the values to merge.

        Returns:
            Dict[str, Any]: A new dictionary containing the merged properties.
        """
        merged_config = deepcopy(current_config)

        for key, value in updated_config.items():
            if isinstance(value, dict):
                # We are handling pre/post actions, so we merge all the actions defined
                merged_config[key] = cls.__merge_config(merged_config[key], value)
            elif isinstance(value, list):
                # We deal with sets of action functions
                if key not in merged_config:
                    merged_config[key] = []

                cast(List, merged_config[key]).extend(value)
            elif isinstance(value, set):
                # We are dealing with labels, so merge them to allow for label inheritance
                merged_config[key] = cast(Set, merged_config[key]).union(value)
            else:
                merged_config[key] = value

        return merged_config
