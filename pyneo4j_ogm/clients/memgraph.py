from typing import List, Self, Union

from pyneo4j_ogm.clients.base import Pyneo4jClient, ensure_initialized
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.memgraph import (
    MemgraphConstraintType,
    MemgraphDataType,
    MemgraphDataTypeMapping,
    MemgraphIndexType,
)


class MemgraphClient(Pyneo4jClient):
    """
    Memgraph client used for interacting with a Memgraph database. Provides basic functionality for querying, indexing,
    constraints and other utilities.
    """

    async def drop_constraints(self) -> Self:
        logger.debug("Discovering constraints")
        constraints, _ = await self.cypher("SHOW CONSTRAINT INFO", auto_committing=True)

        if len(constraints) == 0:
            return self

        logger.warning("Dropping %d constraints", len(constraints))
        for constraint in constraints:
            match constraint[0]:
                case MemgraphConstraintType.EXISTS.value:
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT EXISTS (n.{constraint[2]})",
                        auto_committing=True,
                    )
                case MemgraphConstraintType.UNIQUE.value:
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT {', '.join([f'n.{constraint_property}' for constraint_property in constraint[2]])} IS UNIQUE",
                        auto_committing=True,
                    )
                case MemgraphConstraintType.DATA_TYPE.value:
                    # Some data types in Memgraph are returned differently that what is used when creating them
                    # Because of that we have to do some additional mapping when dropping them
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT n.{constraint[2]} IS TYPED {MemgraphDataTypeMapping[constraint[3]]}",
                        auto_committing=True,
                    )

        logger.info("%d constraints dropped", len(constraints))
        return self

    async def drop_indexes(self) -> Self:
        logger.debug("Discovering indexes")
        indexes, _ = await self.cypher("SHOW INDEX INFO", auto_committing=True)

        if len(indexes) == 0:
            return self

        logger.warning("Dropping %d indexes", len(indexes))
        for index in indexes:
            match index[0]:
                case MemgraphIndexType.EDGE_TYPE.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.EDGE_TYPE_AND_PROPERTY.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.LABEL.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.LABEL_AND_PROPERTY.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.POINT.value:
                    await self.cypher(f"DROP POINT INDEX ON :{index[1]}({index[2]})", auto_committing=True)

        logger.info("%d indexes dropped", len(indexes))
        return self

    @ensure_initialized
    async def existence_constraint(self, label: str, property_: str) -> Self:
        """
        Creates a new existence constraint for a node with a given label. Can only be used to create existence constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            property_ (str): The property which should be affected by the constraint.

        Returns:
            Self: The client.
        """
        logger.info("Creating existence constraint on %s", label)
        node_pattern = QueryBuilder.node_pattern("n", label)

        logger.debug("Creating existence constraint for %s on property %s", label, property_)
        await self.cypher(f"CREATE CONSTRAINT ON {node_pattern} ASSERT EXISTS (n.{property_})", auto_committing=True)

        return self

    @ensure_initialized
    async def uniqueness_constraint(self, label: str, properties: Union[List[str], str]) -> Self:
        """
        Creates a new uniqueness constraint for a node with a given label. Can only be used to create uniqueness constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            properties (Union[List[str], str]): The properties which should be affected by the constraint.

        Returns:
            Self: The client.
        """
        logger.info("Creating uniqueness constraint on %s", label)
        normalized_properties = [properties] if isinstance(properties, str) else properties

        node_pattern = QueryBuilder.node_pattern("n", label)
        property_pattern = ", ".join(f"n.{property_}" for property_ in normalized_properties)

        logger.debug("Creating uniqueness constraint for %s on properties %s", label, property_pattern)
        await self.cypher(
            f"CREATE CONSTRAINT ON {node_pattern} ASSERT {property_pattern} IS UNIQUE", auto_committing=True
        )

        return self

    @ensure_initialized
    async def data_type_constraint(self, label: str, property_: str, data_type: MemgraphDataType) -> Self:
        """
        Creates a new data type constraint for a node with a given label. Can only be used to create data type constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            property_ (str): The property which should be affected by the constraint.
            data_type (MemgraphDataType): The data type to enforce.

        Raises:
            ClientError: If a data type constraint already exists on the label-property pair.

        Returns:
            Self: The client.
        """
        logger.info("Creating data type constraint on %s for type %s", label, data_type.value)
        node_pattern = QueryBuilder.node_pattern("n", label)

        logger.debug("Creating data type constraint for %s on property %s", label, property_)
        await self.cypher(
            f"CREATE CONSTRAINT ON {node_pattern} ASSERT n.{property_} IS TYPED {data_type.value}",
            auto_committing=True,
        )

        return self

    @ensure_initialized
    async def entity_index(self, label_or_edge: str, entity_type: EntityType) -> Self:
        """
        Creates a label/edge index.

        Args:
            label_or_edge (str): Label/edge in which the index is created.
            entity_type (EntityType): The type of graph entity for which the index will be created.

        Returns:
            Self: The client.
        """
        logger.info("Creating %s index for %s", "label" if entity_type == EntityType.NODE else "edge", label_or_edge)
        await self.cypher(
            f"CREATE {'EDGE ' if entity_type == EntityType.RELATIONSHIP else ''} INDEX ON :{label_or_edge}",
            auto_committing=True,
        )

        return self

    @ensure_initialized
    async def property_index(self, label_or_edge: str, entity_type: EntityType, property_: str) -> Self:
        """
        Creates a label/property or edge/property pair index.

        Args:
            label_or_edge (str): Label/edge in which the index is created.
            entity_type (EntityType): The type of graph entity for which the index will be created.
            property_ (str): The property which should be affected by the index.

        Returns:
            Self: The client.
        """
        logger.info(
            "Creating %s pair index for %s on %s",
            "label" if entity_type == EntityType.NODE else "edge",
            label_or_edge,
            property_,
        )
        await self.cypher(
            f"CREATE {'EDGE ' if entity_type == EntityType.RELATIONSHIP else ''} INDEX ON :{label_or_edge}({property_})",
            auto_committing=True,
        )

        return self

    @ensure_initialized
    async def point_index(self, label: str, property_: str) -> Self:
        """
        Creates a point index.

        Args:
            label (str): Label/edge in which the index is created.
            property_ (str): The property which should be affected by the index.

        Returns:
            Self: The client.
        """
        logger.info(
            "Creating point index for %s on %s",
            label,
            property_,
        )
        await self.cypher(f"CREATE POINT INDEX ON :{label}({property_})", auto_committing=True)

        return self

    @ensure_initialized
    async def _check_database_version(self) -> None:
        # I'm not sure if we actually need/can to check anything here since the server info
        # only states 'Neo4j/v5.11.0 compatible graph database server - Memgraph'
        pass

    @ensure_initialized
    async def _initialize_models(self) -> None:
        # TODO: initialize models when model config is set up
        pass
