from functools import wraps
from typing import Dict, List, Optional, Self, Type, TypedDict, Union, cast
from uuid import uuid4

from neo4j import AsyncDriver

from pyneo4j_ogm.clients.base import Pyneo4jClient, ensure_initialized
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    UnsupportedDatabaseVersionError,
)
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.models.relationship import RelationshipModel
from pyneo4j_ogm.options.field_options import (
    FullTextIndex,
    PointIndex,
    RangeIndex,
    TextIndex,
    UniquenessConstraint,
    VectorIndex,
)
from pyneo4j_ogm.options.model_options import ValidatedNodeConfiguration
from pyneo4j_ogm.pydantic import get_field_options, get_model_fields
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import EntityType


class IndexConstraintMapping(TypedDict):
    labels_or_type: List[str]
    properties: List[str]
    has_labels_or_type_specified: bool


ModelInitializationMapping = Dict[
    Type[Union[UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex]],
    Dict[str, IndexConstraintMapping],
]


def ensure_neo4j_version(major_version: int, minor_version: int, patch_version: int):
    """
    Ensures that the connected Neo4j database has a minimum version. Only usable for
    `Neo4jClient`.

    Args:
        major_version (int): The lowest allowed major version.
        minor_version (int): The lowest allowed minor version.
        patch_version (int): The lowest allowed patch version.
    """

    def decorator(func):

        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            logger.debug("Ensuring client has minimum required version")
            version = cast(Optional[str], getattr(self, "_version", None))
            if version is None:
                raise ClientNotInitializedError()

            major, minor, patch = [int(semver_partial) for semver_partial in version.split(".")]

            if major < major_version or minor < minor_version or patch < patch_version:
                raise UnsupportedDatabaseVersionError()

            result = await func(self, *args, **kwargs)
            return result

        return wrapper

    return decorator


class Neo4jClient(Pyneo4jClient):
    """
    Neo4j client used for interacting with a Neo4j database. Provides basic functionality for querying, indexing,
    constraints and other utilities.
    """

    _version: Optional[str]
    _allow_nested_properties: bool

    async def connect(
        self,
        uri: str,
        *args,
        skip_constraints: bool = False,
        skip_indexes: bool = False,
        allow_nested_properties: bool = True,
        **kwargs,
    ) -> Self:
        self._allow_nested_properties = allow_nested_properties
        return await self.connect(uri, *args, skip_constraints, skip_indexes, **kwargs)

    @ensure_initialized
    async def drop_constraints(self) -> Self:
        logger.debug("Discovering constraints")
        constraints, _ = await self.cypher("SHOW CONSTRAINTS")

        if len(constraints) == 0:
            return self

        logger.warning("Dropping %d constraints", len(constraints))
        for constraint in constraints:
            logger.debug("Dropping constraint %s", constraint[1])
            await self.cypher(f"DROP CONSTRAINT {constraint[1]}")

        logger.info("%d constraints dropped", len(constraints))
        return self

    @ensure_initialized
    async def drop_indexes(self) -> Self:
        logger.debug("Discovering indexes")
        indexes, _ = await self.cypher("SHOW INDEXES")

        if len(indexes) == 0:
            return self

        logger.warning("Dropping %d indexes", len(indexes))
        for index in indexes:
            logger.debug("Dropping index %s", index[1])
            await self.cypher(f"DROP INDEX {index[1]}")

        logger.info("%d indexes dropped", len(indexes))
        return self

    @ensure_initialized
    async def uniqueness_constraint(
        self,
        name: str,
        entity_type: EntityType,
        label_or_type: str,
        properties: Union[List[str], str],
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a uniqueness constraint for a given node or relationship. By default, this will use `IF NOT EXISTS`
        when creating constraints to prevent errors if the constraint already exists. This behavior can be changed by
        passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the constraint.
            entity_type (EntityType): The type of graph entity for which the constraint will be created.
            label_or_type (str): When creating a constraint for a node, the label on which the constraint will be created.
                In case of a relationship, the relationship type.
            properties (Union[List[str], str]): The properties which should be affected by the constraint.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate constraints.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating uniqueness constraint %s on %s", name, label_or_type)
        normalized_properties = [properties] if isinstance(properties, str) else properties

        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", label_or_type)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", label_or_type)

        if len(normalized_properties) == 1:
            properties_pattern = f"e.{normalized_properties[0]}"
        else:
            properties_pattern = f"({', '.join([f'e.{property_}' for property_ in normalized_properties])})"

        logger.debug("Creating uniqueness constraint for %s on properties %s", label_or_type, properties_pattern)
        await self.cypher(
            f"CREATE CONSTRAINT {name}{existence_pattern} FOR {entity_pattern} REQUIRE {properties_pattern} IS UNIQUE"
        )

        return self

    @ensure_initialized
    async def range_index(
        self,
        name: str,
        entity_type: EntityType,
        label_or_type: str,
        properties: Union[List[str], str],
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a range index for the given node or relationship. By default, this will se `IF NOT EXISTS`
        when creating indexes to prevent errors if the index already exists. This behavior can be
        changed by passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the index.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            label_or_type (str): When creating a index for a node, the label on which the index will be created.
                In case of a relationship, the relationship type.
            properties (Union[List[str], str]): The properties which should be affected by the index.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate indexes.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating range index %s for %s", name, label_or_type)
        normalized_properties = [properties] if isinstance(properties, str) else properties

        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"
        properties_pattern = ", ".join(f"e.{property_}" for property_ in normalized_properties)

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", label_or_type)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", label_or_type)

        logger.debug("Creating range index for %s on properties %s", label_or_type, properties_pattern)
        await self.cypher(f"CREATE INDEX {name}{existence_pattern} FOR {entity_pattern} ON ({properties_pattern})")

        return self

    @ensure_initialized
    async def text_index(
        self,
        name: str,
        entity_type: EntityType,
        label_or_type: str,
        property_: str,
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a text index for the given node or relationship. By default, this will se `IF NOT EXISTS`
        when creating indexes to prevent errors if the index already exists. This behavior can be
        changed by passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the index.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            label_or_type (str): When creating a index for a node, the label on which the index will be created.
                In case of a relationship, the relationship type.
            property_ (str): The property which should be affected by the index.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate indexes.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating text index %s for %s", name, label_or_type)
        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", label_or_type)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", label_or_type)

        logger.debug("Creating text index for %s on property %s", label_or_type, property_)
        await self.cypher(f"CREATE TEXT INDEX {name}{existence_pattern} FOR {entity_pattern} ON (e.{property_})")

        return self

    @ensure_initialized
    async def point_index(
        self,
        name: str,
        entity_type: EntityType,
        label_or_type: str,
        property_: str,
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a point index for the given node or relationship. By default, this will se `IF NOT EXISTS`
        when creating indexes to prevent errors if the index already exists. This behavior can be
        changed by passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the index.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            label_or_type (str): When creating a index for a node, the label on which the index will be created.
                In case of a relationship, the relationship type.
            property_ (str): The property which should be affected by the index.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate indexes.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating point index %s for %s", name, label_or_type)
        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", label_or_type)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", label_or_type)

        logger.debug("Creating point index for %s on property %s", label_or_type, property_)
        await self.cypher(f"CREATE POINT INDEX {name}{existence_pattern} FOR {entity_pattern} ON (e.{property_})")

        return self

    @ensure_initialized
    async def fulltext_index(
        self,
        name: str,
        entity_type: EntityType,
        labels_or_types: Union[List[str], str],
        properties: Union[List[str], str],
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a fulltext index for the given node or relationship. By default, this will se `IF NOT EXISTS`
        when creating indexes to prevent errors if the index already exists. This behavior can be
        changed by passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the index.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            labels_or_types (Union[List[str], str]): When creating a index for a node, the labels on which the index will be created.
                In case of a relationship, the relationship types.
            properties (Union[List[str], str]): The properties which should be affected by the index.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate indexes.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating fulltext index %s for %s", name, labels_or_types)
        normalized_properties = [properties] if isinstance(properties, str) else properties

        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"
        properties_pattern = ", ".join(f"e.{property_}" for property_ in normalized_properties)

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", labels_or_types, True)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", labels_or_types)

        logger.debug("Creating fulltext index for %s on properties %s", labels_or_types, properties_pattern)
        await self.cypher(
            f"CREATE FULLTEXT INDEX {name}{existence_pattern} FOR {entity_pattern} ON EACH [{properties_pattern}]"
        )

        return self

    @ensure_initialized
    @ensure_neo4j_version(5, 18, 0)
    async def vector_index(
        self,
        name: str,
        entity_type: EntityType,
        label_or_type: str,
        property_: str,
        raise_on_existing: bool = False,
    ) -> Self:
        """
        Creates a vector index for the given node or relationship. By default, this will se `IF NOT EXISTS`
        when creating indexes to prevent errors if the index already exists. This behavior can be
        changed by passing `raise_on_existing` as `True`.

        Args:
            name (str): The name of the index.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            label_or_type (str): When creating a index for a node, the label on which the index will be created.
                In case of a relationship, the relationship type.
            property_ (str): The property which should be affected by the index.
            raise_on_existing (bool): Whether to use `IF NOT EXISTS` to prevent errors when creating duplicate indexes.
                Defaults to `False`.

        Returns:
            Self: The client.
        """
        logger.info("Creating vector index %s for %s", name, label_or_type)
        existence_pattern = "" if raise_on_existing else " IF NOT EXISTS"

        if entity_type == EntityType.NODE:
            entity_pattern = QueryBuilder.node_pattern("e", label_or_type)
        else:
            entity_pattern = QueryBuilder.relationship_pattern("e", label_or_type)

        logger.debug("Creating vector index for %s on property %s", label_or_type, property_)
        await self.cypher(f"CREATE VECTOR INDEX {name}{existence_pattern} FOR {entity_pattern} ON (e.{property_})")

        return self

    @ensure_initialized
    async def _check_database_version(self) -> None:
        logger.debug("Checking if Neo4j version is supported")
        server_info = await cast(AsyncDriver, self._driver).get_server_info()

        version = server_info.agent.split("/")[1]
        self._version = version

        if int(version.split(".")[0]) < 5:
            raise UnsupportedDatabaseVersionError()

    @ensure_initialized
    async def _initialize_models(self) -> None:
        for model in self._models:
            entity_type = EntityType.NODE if issubclass(model, NodeModel) else EntityType.RELATIONSHIP

            if (model._ogm_config.skip_constraint_creation and model._ogm_config.skip_index_creation) or (
                self._skip_constraint_creation and self._skip_index_creation
            ):
                logger.debug("Constraint and index creation disabled for model %s, skipping", model.__name__)
                continue

            logger.debug("Initializing model %s", model.__name__)
            # This mapping will hold all indexes and constraints we have to create
            mapping: ModelInitializationMapping = {}

            for field_name, field in get_model_fields(model).items():
                options, _ = get_field_options(field)
                self.__generate_options_mapping(model, field_name, options, mapping)

            for index_or_constraint_type, mapped_options in mapping.items():
                for mapped_option in mapped_options.values():
                    joined_property_str = "_".join(mapped_option["properties"])

                    if index_or_constraint_type == UniquenessConstraint:
                        await self.uniqueness_constraint(
                            f"CT_{index_or_constraint_type.__name__}_{mapped_option['labels_or_type'][0]}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"],
                        )
                    elif index_or_constraint_type == RangeIndex:
                        await self.range_index(
                            f"IX_{index_or_constraint_type.__name__}_{mapped_option['labels_or_type'][0]}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"],
                        )
                    elif index_or_constraint_type == TextIndex:
                        await self.text_index(
                            f"IX_{index_or_constraint_type.__name__}_{mapped_option['labels_or_type'][0]}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                        )
                    elif index_or_constraint_type == FullTextIndex:
                        joined_labels_str = "_".join(mapped_option["labels_or_type"])
                        await self.fulltext_index(
                            f"IX_{index_or_constraint_type.__name__}_{joined_labels_str}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"],
                            mapped_option["properties"],
                        )
                    elif index_or_constraint_type == VectorIndex:
                        await self.vector_index(
                            f"IX_{index_or_constraint_type.__name__}_{mapped_option['labels_or_type'][0]}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                        )
                    elif index_or_constraint_type == PointIndex:
                        await self.point_index(
                            f"IX_{index_or_constraint_type.__name__}_{mapped_option['labels_or_type'][0]}_{joined_property_str}",
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                        )

            pass

    def __generate_options_mapping(
        self,
        model: Union[Type[NodeModel], Type[RelationshipModel]],
        field_name: str,
        options: List[Union[UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex]],
        mapping: ModelInitializationMapping,
    ) -> None:
        is_node_model = issubclass(model, NodeModel)

        for option in options:
            has_composite_key = getattr(option, "composite_key", None) is not None
            map_key = getattr(option, "composite_key") if has_composite_key else str(uuid4())

            if type(option) not in mapping:
                mapping[type(option)] = {}

            mapped_options = mapping[type(option)]

            types_to_check = []
            if not (model._ogm_config.skip_constraint_creation or self._skip_constraint_creation):
                types_to_check.append(UniquenessConstraint)
            if not (model._ogm_config.skip_index_creation or self._skip_index_creation):
                types_to_check.extend([TextIndex, PointIndex, RangeIndex, VectorIndex])

            if has_composite_key and map_key in mapped_options:
                mapped_options[map_key]["properties"].append(field_name)

            # FullTextIndex is the only index with different structure since it can create the same index
            # on multiple labels
            if isinstance(option, tuple(types_to_check)):
                specified_label: Optional[str] = None if not is_node_model else getattr(option, "specified_label", None)
                has_specified_label = specified_label is not None

                # Validate that the provided specified_label is actually one defined on the model
                if is_node_model and has_specified_label and specified_label not in model._ogm_config.labels:
                    raise ValueError(f"'{specified_label}' is not a valid label for model {model.__name__}")

                # Define defaults for each option we come across to make handling easier later on
                if map_key not in mapped_options:
                    mapped_options[map_key] = {
                        "labels_or_type": [model._ogm_config.labels[0] if is_node_model else model._ogm_config.type],
                        "properties": [field_name],
                        "has_labels_or_type_specified": False,
                    }

                # If we are handling a composite key and the specified labels diverge, we throw a error since we
                # don't know what label to use
                if (
                    has_specified_label
                    and mapped_options[map_key]["has_labels_or_type_specified"]
                    and specified_label != mapped_options[map_key]["labels_or_type"][0]
                ):
                    raise ValueError(
                        f"Multiple different labels/types defined for composite key {map_key} in model {model.__name__}"
                    )

                # If no custom labels have been defined, we either use the existing label if we are currently handling
                # a composite key or we generate a random UUID
                if has_specified_label:
                    mapped_options[map_key]["labels_or_type"] = [specified_label]
                    mapped_options[map_key]["has_labels_or_type_specified"] = True
            elif not (model._ogm_config.skip_index_creation or self._skip_index_creation) and isinstance(
                option, FullTextIndex
            ):
                specified_labels: Optional[Union[List[str], str]] = (
                    None if not is_node_model else getattr(option, "specified_labels", None)
                )
                has_specified_labels = specified_labels is not None

                if specified_labels is not None and isinstance(specified_labels, str):
                    specified_labels = [specified_labels]

                # Validate that the provided specified_label is actually one defined on the model
                if is_node_model and has_specified_labels:
                    invalid_labels = [
                        label
                        for label in cast(List[str], specified_labels)
                        if label not in cast(ValidatedNodeConfiguration, model._ogm_config).labels
                    ]

                    if len(invalid_labels) > 0:
                        raise ValueError(
                            f"Labels {', '.join(invalid_labels)} are not a valid label for model {model.__name__}"
                        )

                # Define defaults for each option we come across to make handling easier later on
                if map_key not in mapped_options:
                    mapped_options[map_key] = {
                        "labels_or_type": (model._ogm_config.labels if is_node_model else [model._ogm_config.type]),
                        "properties": [field_name],
                        "has_labels_or_type_specified": False,
                    }

                    # If we are handling a composite key and the specified labels diverge, we throw a error since we
                # don't know what label to use
                if (
                    has_specified_labels
                    and mapped_options[map_key]["has_labels_or_type_specified"]
                    and set(cast(List[str], specified_labels)) != set(mapped_options[map_key]["labels_or_type"])
                ):
                    raise ValueError(f"Multiple different labels/types defined for composite key {map_key}")

                if has_specified_labels:
                    mapped_options[map_key]["labels_or_type"] = cast(List[str], specified_labels)
                    mapped_options[map_key]["has_labels_or_type_specified"] = True
