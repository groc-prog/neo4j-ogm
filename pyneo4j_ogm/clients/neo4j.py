from functools import wraps
from typing import List, Optional, Self, Union, cast

from neo4j import AsyncDriver

from pyneo4j_ogm.clients.base import Pyneo4jClient, ensure_initialized
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    UnsupportedDatabaseVersionError,
)
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import EntityType


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
        label_or_type: str,
        entity_type: EntityType,
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
        label_or_type: str,
        entity_type: EntityType,
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
        label_or_type: str,
        entity_type: EntityType,
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
        labels_or_types: Union[List[str], str],
        entity_type: EntityType,
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
        label_or_type: str,
        entity_type: EntityType,
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
        # TODO: initialize models when model config is set up
        pass
