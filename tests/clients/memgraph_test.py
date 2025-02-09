# pylint: disable=missing-class-docstring, redefined-outer-name, unused-import, unused-argument, broad-exception-raised, line-too-long

import asyncio
from datetime import date
from os import path
from typing import Any, Dict, List, Union
from unittest.mock import AsyncMock, MagicMock, patch

import neo4j
import neo4j.graph
import pytest
from neo4j.exceptions import ClientError
from pydantic import BaseModel
from typing_extensions import Annotated

from pyneo4j_ogm.clients.memgraph import MemgraphClient
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    DuplicateModelError,
    ModelResolveError,
    NoTransactionInProgressError,
)
from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.models.path import PathContainer
from pyneo4j_ogm.models.relationship import RelationshipModel
from pyneo4j_ogm.options.field_options import (
    DataTypeConstraint,
    ExistenceConstraint,
    PointIndex,
    PropertyIndex,
    UniquenessConstraint,
)
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.memgraph import MemgraphDataType
from tests.fixtures.db import (
    Authentication,
    ConnectionString,
    memgraph_client,
    memgraph_session,
)
from tests.fixtures.registry import reset_registry_state


async def setup_constraints(session: neo4j.AsyncSession):
    query = await session.run("CREATE CONSTRAINT ON (n:Employee) ASSERT n.id IS UNIQUE")
    await query.consume()
    query = await session.run("CREATE CONSTRAINT ON (n:Employee) ASSERT n.name, n.address IS UNIQUE")
    await query.consume()
    query = await session.run("CREATE CONSTRAINT ON (n:label) ASSERT EXISTS (n.property)")
    await query.consume()
    query = await session.run("CREATE CONSTRAINT ON (n:label) ASSERT n.property IS TYPED STRING")
    await query.consume()

    query = await session.run("SHOW CONSTRAINT INFO")
    constraints = await query.values()
    await query.consume()
    assert len(constraints) == 4


async def check_no_constraints(session: neo4j.AsyncSession):
    query = await session.run("SHOW CONSTRAINT INFO")
    constraints = await query.values()
    await query.consume()
    assert len(constraints) == 0


async def setup_indexes(session: neo4j.AsyncSession):
    query = await session.run("CREATE INDEX ON :Person")
    await query.consume()
    query = await session.run("CREATE INDEX ON :Person(age)")
    await query.consume()
    query = await session.run("CREATE EDGE INDEX ON :EDGE_TYPE")
    await query.consume()
    query = await session.run("CREATE EDGE INDEX ON :EDGE_TYPE(property_name)")
    await query.consume()
    query = await session.run("CREATE POINT INDEX ON :Label(property)")
    await query.consume()

    query = await session.run("SHOW INDEX INFO")
    constraints = await query.values()
    await query.consume()
    assert len(constraints) == 5


async def check_no_indexes(session: neo4j.AsyncSession):
    query = await session.run("SHOW INDEX INFO")
    constraints = await query.values()
    await query.consume()
    assert len(constraints) == 0


class TestMemgraphConnection:
    async def test_successful_connect(self):
        client = MemgraphClient()
        await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)

    async def test_checks_connectivity_and_auth(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch(
            "neo4j.AsyncDriver.verify_connectivity", wraps=async_driver_mock.verify_connectivity
        ) as verify_connectivity_spy:
            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)

            verify_connectivity_spy.assert_called()

    async def test_raises_on_invalid_uri(self):
        with pytest.raises(ValueError):
            client = MemgraphClient()
            await client.connect("bolt://invalid-connection:9999")

    async def test_raises_on_invalid_auth(self):
        with pytest.raises(ClientError):
            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value)

    async def test_connected_state(self):
        client = MemgraphClient()

        is_connected = await client.connected()
        assert is_connected is False

        await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
        is_connected = await client.connected()
        assert is_connected is True

    async def test_connected_on_connection_failure(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch(
            "neo4j.AsyncDriver.verify_connectivity", wraps=async_driver_mock.verify_connectivity
        ) as verify_connectivity_spy:
            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)

            verify_connectivity_spy.side_effect = RuntimeError("Mocked failure")

            is_connected = await client.connected()
            assert is_connected is False

    async def test_close_raises_if_not_initialized(self):
        client = MemgraphClient()

        with pytest.raises(ClientNotInitializedError):
            await client.close()

    async def test_closes_driver_connection(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch("neo4j.AsyncDriver.close", wraps=async_driver_mock.close) as close_spy:
            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)

            await client.close()
            close_spy.assert_called()
            assert client._driver is None


class TestMemgraphConstraints:
    class TestMemgraphExistenceConstraint:
        async def test_existence_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.existence_constraint("Person", "id")

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "exists"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"

    class TestMemgraphUniquenessConstraint:
        async def test_uniqueness_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.uniqueness_constraint("Person", "id")

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "unique"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == ["id"]

        async def test_uniqueness_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.uniqueness_constraint("Person", ["id", "age"])

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "unique"
            assert constraints[0][1] == "Person"
            assert set(constraints[0][2]) == set(["id", "age"])

    class TestMemgraphDataTypeConstraint:
        async def test_throws_non_constraint_errors(self, memgraph_client):
            def patched_cypher_fn(*args, **kwargs):
                raise Exception()  # pylint: disable=broad-exception-raised

            with patch.object(memgraph_client, "cypher", patched_cypher_fn):
                with pytest.raises(Exception):
                    await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.BOOLEAN)

        async def test_data_type_constraint_does_not_throw_on_duplicate(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.BOOLEAN)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.BOOLEAN)

        async def test_bool_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.BOOLEAN)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "BOOL"

        async def test_string_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.STRING)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "STRING"

        async def test_string_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.STRING)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "STRING"

        async def test_int_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.INTEGER)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "INTEGER"

        async def test_int_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.INTEGER)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "INTEGER"

        async def test_float_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.FLOAT)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "FLOAT"

        async def test_float_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.FLOAT)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "FLOAT"

        async def test_list_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LIST)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LIST"

        async def test_list_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LIST)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LIST"

        async def test_map_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.MAP)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "MAP"

        async def test_map_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.MAP)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "MAP"

        async def test_duration_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DURATION)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "DURATION"

        async def test_duration_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DURATION)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "DURATION"

        async def test_date_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DATE)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "DATE"

        async def test_date_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DATE)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "DATE"

        async def test_local_time_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_TIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LOCAL TIME"

        async def test_local_time_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_TIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LOCAL TIME"

        async def test_local_datetime_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_DATETIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LOCAL DATE TIME"

        async def test_local_datetime_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_DATETIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "LOCAL DATE TIME"

        async def test_zoned_datetime_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ZONED_DATETIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "ZONED DATE TIME"

        async def test_zoned_datetime_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ZONED_DATETIME)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "ZONED DATE TIME"

        async def test_enum_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ENUM)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "ENUM"

        async def test_enum_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ENUM)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "ENUM"

        async def test_point_data_type_constraint(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.POINT)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "POINT"

        async def test_point_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)
            await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.POINT)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "id"
            assert constraints[0][3] == "POINT"


class TestMemgraphIndexes:
    class TestMemgraphEntityIndex:
        async def test_node_index(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)
            await memgraph_client.entity_index("Person", EntityType.NODE)

            query = await memgraph_session.run("SHOW INDEX INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "label"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] is None

        async def test_relationship_index(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)
            await memgraph_client.entity_index("Person", EntityType.RELATIONSHIP)

            query = await memgraph_session.run("SHOW INDEX INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "edge-type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] is None

    class TestMemgraphPropertyIndex:
        async def test_node_property_index(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)
            await memgraph_client.property_index(EntityType.NODE, "Person", "age")

            query = await memgraph_session.run("SHOW INDEX INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "label+property"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "age"

        async def test_relationship_property_index(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)
            await memgraph_client.property_index(EntityType.RELATIONSHIP, "Person", "age")

            query = await memgraph_session.run("SHOW INDEX INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "edge-type+property"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "age"

    class TestMemgraphPointIndex:
        async def test_point_index(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)
            await memgraph_client.point_index("Person", "age")

            query = await memgraph_session.run("SHOW INDEX INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "point"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "age"


class TestMemgraphQueries:
    class TestMemgraphUtilities:
        async def test_drop_constraints(self, memgraph_session, memgraph_client):
            await setup_constraints(memgraph_session)
            await memgraph_client.drop_constraints()

            await check_no_constraints(memgraph_session)

        async def test_does_nothing_if_no_constraints_defined(self, memgraph_session, memgraph_client):
            await check_no_constraints(memgraph_session)

            with patch(
                "pyneo4j_ogm.clients.memgraph.MemgraphClient.cypher", wraps=memgraph_client.cypher
            ) as cypher_spy:
                await memgraph_client.drop_constraints()

                cypher_spy.assert_called_once()

        async def test_drop_indexes(self, memgraph_session, memgraph_client):
            await setup_indexes(memgraph_session)
            await memgraph_client.drop_indexes()

            await check_no_indexes(memgraph_session)

        async def test_does_nothing_if_no_index_defined(self, memgraph_session, memgraph_client):
            await check_no_indexes(memgraph_session)

            with patch(
                "pyneo4j_ogm.clients.memgraph.MemgraphClient.cypher", wraps=memgraph_client.cypher
            ) as cypher_spy:
                await memgraph_client.drop_indexes()

                cypher_spy.assert_called_once()

        async def test_drop_nodes(self, memgraph_client, memgraph_session):
            query = await memgraph_session.run("CREATE (:Person), (:Worker), (:People)-[:LOVES]->(:Coffee)")
            await query.consume()
            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()
            assert len(result) == 4

            await memgraph_client.drop_nodes()

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()
            assert len(result) == 0

    class TestMemgraphBatching:
        async def test_batching(self, memgraph_client, memgraph_session):
            async with memgraph_client.batching():
                await memgraph_client.cypher("CREATE (:Developer)")
                await memgraph_client.cypher("CREATE (:Coffee)")
                await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

                query = await memgraph_session.run("MATCH (n) RETURN n")
                result = await query.values()
                await query.consume()
                assert len(result) == 0

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()
            assert len(result) == 2

        async def test_batching_query(self, memgraph_client, memgraph_session):
            query = await memgraph_session.run("CREATE (:Developer)")
            await query.consume()
            query = await memgraph_session.run("CREATE (:Coffee)")
            await query.consume()

            async with memgraph_client.batching():
                results, _ = await memgraph_client.cypher("MATCH (n) RETURN n", resolve_models=False)
                assert len(results) == 2

                for result in results:
                    assert isinstance(result[0], neo4j.graph.Node)

        async def test_batching_rolls_back_on_error(self, memgraph_client):
            with patch.object(neo4j.AsyncTransaction, "rollback", new=AsyncMock()) as mock_rollback:
                try:
                    async with memgraph_client.batching():
                        raise Exception()
                except Exception:
                    pass

                mock_rollback.assert_awaited_once()

        async def test_batching_using_same_transaction(self, memgraph_client):
            memgraph_client._driver.session = MagicMock(wraps=memgraph_client._driver.session)

            async with memgraph_client.batching():
                await memgraph_client.cypher("CREATE (:Developer)")
                await memgraph_client.cypher("CREATE (:Coffee)")
                await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

            assert memgraph_client._driver.session.call_count == 1

        async def test_batching_raises_on_shared_session_missing_when_committing(self, memgraph_client):
            with pytest.raises(NoTransactionInProgressError):
                async with memgraph_client.batching():
                    await memgraph_client.cypher("CREATE (:Developer)")
                    await memgraph_client.cypher("CREATE (:Coffee)")
                    await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

                    memgraph_client._session = None

        async def test_batching_raises_on_shared_transaction_missing_when_committing(self, memgraph_client):
            with pytest.raises(NoTransactionInProgressError):
                async with memgraph_client.batching():
                    await memgraph_client.cypher("CREATE (:Developer)")
                    await memgraph_client.cypher("CREATE (:Coffee)")
                    await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

                    memgraph_client._transaction = None

        async def test_batching_raises_on_shared_session_missing_when_rolling_back(self, memgraph_client):
            with patch("time.perf_counter", side_effect=RuntimeError("perf_counter failed")):
                with pytest.raises(NoTransactionInProgressError):
                    async with memgraph_client.batching():
                        memgraph_client._session = None

                        await memgraph_client.cypher("CREATE (:Developer)")
                        await memgraph_client.cypher("CREATE (:Coffee)")
                        await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

        async def test_batching_raises_on_shared_transaction_missing_when_rolling_back(self, memgraph_client):
            with patch("time.perf_counter", side_effect=RuntimeError("perf_counter failed")):
                with pytest.raises(NoTransactionInProgressError):
                    async with memgraph_client.batching():
                        memgraph_client._transaction = None

                        await memgraph_client.cypher("CREATE (:Developer)")
                        await memgraph_client.cypher("CREATE (:Coffee)")
                        await memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

    class TestMemgraphCypherQueries:
        async def test_cypher(self, memgraph_client, memgraph_session):
            labels = ["Developer", "Coffee"]
            result, keys = await memgraph_client.cypher(f"CREATE (n:{labels[0]}), (m:{labels[1]})")

            assert len(result) == 0
            assert len(keys) == 0

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 2
            assert len(result[0][0].labels) == 1
            assert len(result[0][0].labels) == 1
            assert list(result[1][0].labels)[0] in labels
            assert list(result[1][0].labels)[0] in labels

        async def test_cypher_uses_unique_transaction(self, memgraph_client):
            memgraph_client._driver.session = MagicMock(wraps=memgraph_client._driver.session)

            coroutine_one = memgraph_client.cypher("CREATE (:Developer)")
            coroutine_two = memgraph_client.cypher("CREATE (:Coffee)")
            coroutine_three = memgraph_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

            await asyncio.gather(coroutine_one, coroutine_two, coroutine_three)

            assert memgraph_client._driver.session.call_count == 3

        async def test_cypher_with_params(self, memgraph_client, memgraph_session):
            result, keys = await memgraph_client.cypher("CREATE (n:Person) SET n.age = $age", {"age": 24})

            assert len(result) == 0
            assert len(keys) == 0

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert len(result[0][0].labels) == 1
            assert list(result[0][0].labels)[0] == "Person"
            assert dict(result[0][0])["age"] == 24

        async def test_cypher_auto_committing(self, memgraph_client, memgraph_session):
            labels = ["Developer", "Coffee"]
            result, keys = await memgraph_client.cypher(
                f"CREATE (n:{labels[0]}), (m:{labels[1]})", auto_committing=True
            )

            assert len(result) == 0
            assert len(keys) == 0

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 2
            assert len(result[0][0].labels) == 1
            assert len(result[0][0].labels) == 1
            assert list(result[1][0].labels)[0] in labels
            assert list(result[1][0].labels)[0] in labels

        async def test_cypher_with_params_auto_committing(self, memgraph_client, memgraph_session):
            result, keys = await memgraph_client.cypher(
                "CREATE (n:Person) SET n.age = $age", {"age": 24}, auto_committing=True
            )

            assert len(result) == 0
            assert len(keys) == 0

            query = await memgraph_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert len(result[0][0].labels) == 1
            assert list(result[0][0].labels)[0] == "Person"
            assert dict(result[0][0])["age"] == 24

    class TestMemgraphResolvingModels:
        async def test_resolves_model_correctly(self, memgraph_client, memgraph_session):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)
            results, _ = await memgraph_client.cypher("MATCH (n)-[r]->(m) RETURN n, m, r")

            assert len(results[0]) == 3

            assert isinstance(results[0][0], Person)
            assert results[0][0].id is not None
            assert results[0][0].element_id is not None
            assert results[0][0].name == person_one["name"]
            assert results[0][0].age == person_one["age"]
            assert results[0][0].is_happy == person_one["is_happy"]

            assert isinstance(results[0][1], Person)
            assert results[0][1].id is not None
            assert results[0][1].element_id is not None
            assert results[0][1].name == person_two["name"]
            assert results[0][1].age == person_two["age"]
            assert results[0][1].is_happy == person_two["is_happy"]

            assert isinstance(results[0][2], Related)
            assert results[0][2].id is not None
            assert results[0][2].element_id is not None
            assert results[0][2].days_since == related["days_since"]
            assert results[0][2].close_friend == related["close_friend"]

        async def test_uses_cached_resolved_models(self, memgraph_client, memgraph_session):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)

            with patch.object(Person, "_inflate", wraps=Person._inflate) as person_spy:
                with patch.object(Related, "_inflate", wraps=Related._inflate) as related_spy:
                    await memgraph_client.cypher("MATCH (n)-[r]->(m) RETURN [n, n, m, m, r, r]")

                    assert person_spy.call_count == 2
                    assert related_spy.call_count == 1

        async def test_resolve_nested_structures(self, memgraph_client, memgraph_session):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)
            results, _ = await memgraph_client.cypher(
                "MATCH (n)-[r]->(m) RETURN [n, {end_node: m}, {nested: {relationship: r}}]"
            )

            assert isinstance(results[0][0][0], Person)
            assert results[0][0][0].age == 24
            assert isinstance(results[0][0][1]["end_node"], Person)
            assert results[0][0][1]["end_node"].age == 53
            assert isinstance(results[0][0][2]["nested"]["relationship"], Related)
            assert results[0][0][2]["nested"]["relationship"].days_since == 213

        async def test_raises_on_unknown_node_model(self, memgraph_client, memgraph_session):
            query = await memgraph_session.run("CREATE (:Node)")
            await query.consume()

            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH (n) RETURN n")

        async def test_raises_on_unknown_relationship_model(self, memgraph_client, memgraph_session):
            query = await memgraph_session.run("CREATE (:Node)-[:RELATION]->(:Node)")
            await query.consume()

            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH ()-[r]->() RETURN r")

        async def test_resolves_paths_correctly(self, memgraph_client, memgraph_session):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)
            results, _ = await memgraph_client.cypher("MATCH path=(n)-[r]->(m) RETURN path")

            assert len(results[0]) == 1
            assert isinstance(results[0][0], PathContainer)
            assert results[0][0].graph is not None
            assert isinstance(results[0][0].start_node, Person)
            assert isinstance(results[0][0].end_node, Person)
            assert all(isinstance(node, Person) for node in results[0][0].nodes)
            assert all(isinstance(relationship, Related) for relationship in results[0][0].relationships)

        async def test_resolves_relationship_start_and_end_nodes_correctly(self, memgraph_client, memgraph_session):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)
            results, _ = await memgraph_client.cypher("MATCH (n)-[r]->(m) RETURN r, n, m")

            assert isinstance(results[0][0], RelationshipModel)
            assert isinstance(results[0][0].start_node, Person)
            assert results[0][0].start_node.element_id == results[0][1].element_id
            assert isinstance(results[0][0].end_node, Person)
            assert results[0][0].end_node.element_id == results[0][2].element_id

        async def test_does_not_resolve_start_and_end_node_if_not_returned_from_query(
            self, memgraph_client, memgraph_session
        ):
            class Person(NodeModel):
                age: int
                name: str
                is_happy: bool

            class Related(RelationshipModel):
                days_since: int
                close_friend: bool

            person_one = {"age": 24, "name": "Bobby Tables", "is_happy": True}
            person_two = {"age": 53, "name": "John Doe", "is_happy": False}
            related = {"days_since": 213, "close_friend": True}

            query = await memgraph_session.run(
                "CREATE (:Person {age: $p_one_age, name: $p_one_name, is_happy: $p_one_is_happy})-[:RELATED {days_since: $related_days_since, close_friend: $related_close_friend}]->(:Person {age: $p_two_age, name: $p_two_name, is_happy: $p_two_is_happy})",
                {
                    "p_one_age": person_one["age"],
                    "p_one_name": person_one["name"],
                    "p_one_is_happy": person_one["is_happy"],
                    "p_two_age": person_two["age"],
                    "p_two_name": person_two["name"],
                    "p_two_is_happy": person_two["is_happy"],
                    "related_days_since": related["days_since"],
                    "related_close_friend": related["close_friend"],
                },
            )
            await query.consume()

            await memgraph_client.register_models(Person, Related)
            results, _ = await memgraph_client.cypher("MATCH ()-[r]->() RETURN r")

            assert isinstance(results[0][0], RelationshipModel)
            assert results[0][0].start_node is None
            assert results[0][0].end_node is None

    async def test_resolves_nested_properties(self, memgraph_client, memgraph_session):
        class DeeplyNested(BaseModel):
            nested_count: int

        class Nested(BaseModel):
            is_nested: bool
            deeply_nested_items: List[DeeplyNested]
            deeply_nested_once: DeeplyNested

        class Person(NodeModel):
            nested: Nested
            id_: int

        query = await memgraph_session.run(
            "CREATE (n:Person {id_: $id, nested: $nested})",
            {
                "id": 1,
                "nested": {
                    "is_nested": True,
                    "deeply_nested_once": {"nested_count": 1},
                    "deeply_nested_items": [
                        {"nested_count": 2},
                        {"nested_count": 4},
                    ],
                },
            },
        )
        await query.consume()

        await memgraph_client.register_models(Person)
        results, _ = await memgraph_client.cypher("MATCH (n:Person) RETURN n")

        assert len(results[0]) == 1
        assert isinstance(results[0][0], Person)
        assert results[0][0].id_ == 1
        assert results[0][0].nested.is_nested
        assert results[0][0].nested.deeply_nested_once.nested_count == 1
        assert len(results[0][0].nested.deeply_nested_items) == 2
        assert results[0][0].nested.deeply_nested_items[0].nested_count == 2
        assert results[0][0].nested.deeply_nested_items[1].nested_count == 4

    async def test_resolves_non_homogeneous_lists(self, memgraph_client, memgraph_session):
        class RandomList(NodeModel):
            items: Union[str, int, bool, Dict[str, Any], List[Any]]

        query = await memgraph_session.run(
            "CREATE (n:RandomList {items: $items})",
            {"items": ["random", 12, False, {"random_key": "something"}, [True, 4, 5.1]]},
        )
        await query.consume()

        await memgraph_client.register_models(RandomList)
        results, _ = await memgraph_client.cypher("MATCH (n:RandomList) RETURN n")

        assert len(results[0]) == 1
        assert isinstance(results[0][0], RandomList)
        assert len(results[0][0].items) == 5
        assert results[0][0].items[0] == "random"
        assert results[0][0].items[1] == 12
        assert not results[0][0].items[2]
        assert results[0][0].items[3] == {"random_key": "something"}
        assert results[0][0].items[4] == [True, 4, 5.1]

    async def test_raises_on_failed_node_inflate(self, memgraph_client, memgraph_session):
        class Person(NodeModel):
            id_: int

        query = await memgraph_session.run(
            "CREATE (:Person {id_: $id_one})",
            {"id_one": 1},
        )
        await query.consume()

        await memgraph_client.register_models(Person)

        with patch.object(Person, "_inflate", side_effect=Exception()):
            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH (n:Person) RETURN n")

    async def test_raises_on_failed_relationship_inflate(self, memgraph_client, memgraph_session):
        class Person(NodeModel):
            id_: int

        class Knows(RelationshipModel):
            status: str

        query = await memgraph_session.run(
            "CREATE (:Person {id_: $id_one})-[:KNOWS {status: $status}]->(:Person {id_: $id_two})",
            {"id_one": 1, "id_two": 2, "status": "OK"},
        )
        await query.consume()

        await memgraph_client.register_models(Person, Knows)

        with patch.object(Knows, "_inflate", side_effect=Exception()):
            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH ()-[r:KNOWS]->() RETURN r")

    async def test_raises_on_failed_relationship_start_or_end_node_inflate(self, memgraph_client, memgraph_session):
        class Person(NodeModel):
            id_: int

        class Knows(RelationshipModel):
            status: str

        query = await memgraph_session.run(
            "CREATE (:Person {id_: $id_one})-[:KNOWS {status: $status}]->(:Person {id_: $id_two})",
            {"id_one": 1, "id_two": 2, "status": "OK"},
        )
        await query.consume()

        await memgraph_client.register_models(Person, Knows)

        with patch.object(Person, "_inflate", side_effect=Exception()):
            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH (n)-[r:KNOWS]->(m) RETURN r, n, m")

    async def test_raises_on_failed_path_node_inflate(self, memgraph_client, memgraph_session):
        class Person(NodeModel):
            id_: int

        class Knows(RelationshipModel):
            status: str

        query = await memgraph_session.run(
            "CREATE (:Person {id_: $id_one})-[:KNOWS {status: $status}]->(:Person {id_: $id_two})",
            {"id_one": 1, "id_two": 2, "status": "OK"},
        )
        await query.consume()

        await memgraph_client.register_models(Person, Knows)

        with patch.object(Person, "_inflate", side_effect=Exception()):
            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH path=(:Person)-[:KNOWS]->(:Person) RETURN path")

    async def test_raises_on_failed_path_relationship_inflate(self, memgraph_client, memgraph_session):
        class Person(NodeModel):
            id_: int

        class Knows(RelationshipModel):
            status: str

        query = await memgraph_session.run(
            "CREATE (:Person {id_: $id_one})-[:KNOWS {status: $status}]->(:Person {id_: $id_two})",
            {"id_one": 1, "id_two": 2, "status": "OK"},
        )
        await query.consume()

        await memgraph_client.register_models(Person, Knows)

        with patch.object(Knows, "_inflate", side_effect=Exception()):
            with pytest.raises(ModelResolveError):
                await memgraph_client.cypher("MATCH path=(:Person)-[:KNOWS]->(:Person) RETURN path")


class TestMemgraphModelInitialization:
    async def test_skips_initialization_if_all_client_skips_defined(self, memgraph_session):
        class Person(NodeModel):
            uid: Annotated[str, UniquenessConstraint(), PropertyIndex()]

        class Likes(RelationshipModel):
            uid: Annotated[str, UniquenessConstraint(), PropertyIndex()]

        client = MemgraphClient()
        await client.connect(
            ConnectionString.MEMGRAPH.value,
            auth=Authentication.MEMGRAPH.value,
            skip_constraints=True,
            skip_indexes=True,
        )
        await client.register_models(Person, Likes)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()
        await query.consume()

        assert len(constraints) == 0

        query = await memgraph_session.run("SHOW INDEX INFO")
        indexes = await query.values()
        await query.consume()

        assert len(indexes) == 0

    async def test_skips_initialization_if_all_model_skips_defined(self, memgraph_session):
        class Person(NodeModel):
            uid: Annotated[str, UniquenessConstraint(), PropertyIndex()]

            ogm_config = {"skip_constraint_creation": True, "skip_index_creation": True}

        class Likes(RelationshipModel):
            uid: Annotated[str, UniquenessConstraint(), PropertyIndex()]

            ogm_config = {"skip_constraint_creation": True, "skip_index_creation": True}

        client = MemgraphClient()
        await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
        await client.register_models(Person, Likes)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()
        await query.consume()

        assert len(constraints) == 0

        query = await memgraph_session.run("SHOW INDEX INFO")
        indexes = await query.values()
        await query.consume()

        assert len(indexes) == 0

    class TestMemgraphUniquenessConstraint:
        async def test_model_registration_with_client_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

            client = MemgraphClient()
            await client.connect(
                ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value, skip_constraints=True
            )
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_model_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_uniqueness_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Person":
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == ["uid"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "LIKES"
                assert constraints[1][2] == ["uid"]
            else:
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "LIKES"
                assert constraints[0][2] == ["uid"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_multi_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "unique"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "unique"
            assert constraints[0][1] == "Human"
            assert constraints[0][2] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_invalid_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_uniqueness_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]
                age: Annotated[int, UniquenessConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][2] == ["uid"]:
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == ["uid"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == ["age"]
            else:
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == ["age"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == ["uid"]

        async def test_model_registration_with_multiple_uniqueness_constraint_specified_labels(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human")]
                age: Annotated[int, UniquenessConstraint(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Human":
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "Human"
                assert constraints[0][2] == ["uid"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == ["age"]
            else:
                assert constraints[0][0] == "unique"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == ["age"]

                assert constraints[1][0] == "unique"
                assert constraints[1][1] == "Human"
                assert constraints[1][2] == ["uid"]

        async def test_model_registration_with_multiple_uniqueness_constraint_invalid_composite_key(
            self, memgraph_session
        ):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human", composite_key="key")]
                age: Annotated[int, UniquenessConstraint(specified_label="Person", composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_uniqueness_constraint_composite_key(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(composite_key="key")]
                age: Annotated[int, UniquenessConstraint(composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert constraints[0][0] == "unique"
            assert constraints[0][1] == "Person"
            assert len(constraints[0][2]) == 2
            assert "age" in constraints[0][2]
            assert "uid" in constraints[0][2]

    class TestMemgraphExistenceConstraint:
        async def test_model_registration_with_client_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, ExistenceConstraint()]

            client = MemgraphClient()
            await client.connect(
                ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value, skip_constraints=True
            )
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_model_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, ExistenceConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_existence_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, ExistenceConstraint()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Person":
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "uid"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "LIKES"
                assert constraints[1][2] == "uid"
            else:
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "LIKES"
                assert constraints[0][2] == "uid"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "uid"

        async def test_model_registration_with_existence_constraint_multi_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "exists"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "uid"

        async def test_model_registration_with_existence_constraint_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "exists"
            assert constraints[0][1] == "Human"
            assert constraints[0][2] == "uid"

        async def test_model_registration_with_existence_constraint_invalid_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_existence_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint()]
                age: Annotated[int, ExistenceConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][2] == "uid":
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "uid"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "age"
            else:
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "age"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "uid"

        async def test_model_registration_with_multiple_existence_constraint_specified_labels(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, ExistenceConstraint(specified_label="Human")]
                age: Annotated[int, ExistenceConstraint(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Human":
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "Human"
                assert constraints[0][2] == "uid"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "age"
            else:
                assert constraints[0][0] == "exists"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "age"

                assert constraints[1][0] == "exists"
                assert constraints[1][1] == "Human"
                assert constraints[1][2] == "uid"

    class TestMemgraphDataTypeConstraint:
        async def test_model_registration_with_client_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

            class Likes(RelationshipModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

            client = MemgraphClient()
            await client.connect(
                ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value, skip_constraints=True
            )
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_model_skipped_constraints(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

                ogm_config = {"skip_constraint_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

                ogm_config = {"skip_constraint_creation": True}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_data_type_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

            class Likes(RelationshipModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Person":
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "uid"
                assert constraints[0][3] == "STRING"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "LIKES"
                assert constraints[1][2] == "uid"
                assert constraints[1][3] == "STRING"
            else:
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "LIKES"
                assert constraints[0][2] == "uid"
                assert constraints[0][3] == "STRING"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "uid"
                assert constraints[1][3] == "STRING"

        async def test_model_registration_with_data_type_constraint_multi_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Person"
            assert constraints[0][2] == "uid"
            assert constraints[0][3] == "STRING"

        async def test_model_registration_with_data_type_constraint_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING, specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][0] == "data_type"
            assert constraints[0][1] == "Human"
            assert constraints[0][2] == "uid"
            assert constraints[0][3] == "STRING"

        async def test_model_registration_with_data_type_constraint_invalid_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING, specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_data_type_constraint(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING)]
                age: Annotated[int, DataTypeConstraint(data_type=MemgraphDataType.INTEGER)]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][2] == "uid":
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "uid"
                assert constraints[0][3] == "STRING"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "age"
                assert constraints[1][3] == "INTEGER"
            else:
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "age"
                assert constraints[0][3] == "INTEGER"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "uid"
                assert constraints[1][3] == "STRING"

        async def test_model_registration_with_multiple_data_type_constraint_specified_labels(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, DataTypeConstraint(data_type=MemgraphDataType.STRING, specified_label="Human")]
                age: Annotated[int, DataTypeConstraint(data_type=MemgraphDataType.INTEGER, specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW CONSTRAINT INFO")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2

            if constraints[0][1] == "Human":
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "Human"
                assert constraints[0][2] == "uid"
                assert constraints[0][3] == "STRING"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "Person"
                assert constraints[1][2] == "age"
                assert constraints[1][3] == "INTEGER"
            else:
                assert constraints[0][0] == "data_type"
                assert constraints[0][1] == "Person"
                assert constraints[0][2] == "age"
                assert constraints[0][3] == "INTEGER"

                assert constraints[1][0] == "data_type"
                assert constraints[1][1] == "Human"
                assert constraints[1][2] == "uid"
                assert constraints[1][3] == "STRING"

    class TestMemgraphPropertyIndex:
        async def test_model_registration_with_client_skipped_indexes(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PropertyIndex()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value, skip_indexes=True)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, PropertyIndex()]

                ogm_config = {"skip_index_creation": True}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_property_index(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PropertyIndex()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][1] == "Person":
                assert indexes[0][0] == "label+property"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "edge-type+property"
                assert indexes[1][1] == "LIKES"
                assert indexes[1][2] == "uid"
            else:
                assert indexes[0][0] == "edge-type+property"
                assert indexes[0][1] == "LIKES"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "label+property"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "uid"

        async def test_model_registration_with_property_index_multi_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][0] == "label+property"
            assert indexes[0][1] == "Person"
            assert indexes[0][2] == "uid"

        async def test_model_registration_with_property_index_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][0] == "label+property"
            assert indexes[0][1] == "Human"
            assert indexes[0][2] == "uid"

        async def test_model_registration_with_property_index_invalid_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_property_index(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex()]
                age: Annotated[int, PropertyIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][2] == "uid":
                assert indexes[0][0] == "label+property"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "label+property"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "age"
            else:
                assert indexes[0][0] == "label+property"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "age"

                assert indexes[1][0] == "label+property"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "uid"

        async def test_model_registration_with_multiple_property_index_specified_labels(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PropertyIndex(specified_label="Human")]
                age: Annotated[int, PropertyIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][1] == "Human":
                assert indexes[0][0] == "label+property"
                assert indexes[0][1] == "Human"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "label+property"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "age"
            else:
                assert indexes[0][0] == "label+property"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "age"

                assert indexes[1][0] == "label+property"
                assert indexes[1][1] == "Human"
                assert indexes[1][2] == "uid"

    class TestMemgraphPointIndex:
        async def test_model_registration_with_client_skipped_indexes(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value, skip_indexes=True)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"skip_index_creation": True}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_point_index(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person, Likes)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][1] == "Person":
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "LIKES"
                assert indexes[1][2] == "uid"
            else:
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "LIKES"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "uid"

        async def test_model_registration_with_point_index_multi_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][0] == "point"
            assert indexes[0][1] == "Person"
            assert indexes[0][2] == "uid"

        async def test_model_registration_with_point_index_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][0] == "point"
            assert indexes[0][1] == "Human"
            assert indexes[0][2] == "uid"

        async def test_model_registration_with_point_index_invalid_specified_label(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_point_index(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]
                age: Annotated[int, PointIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][2] == "uid":
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "age"
            else:
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "age"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "uid"

        async def test_model_registration_with_multiple_point_index_specified_labels(self, memgraph_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Human")]
                age: Annotated[int, PointIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = MemgraphClient()
            await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
            await client.register_models(Person)

            query = await memgraph_session.run("SHOW INDEX INFO")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2

            if indexes[0][1] == "Human":
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "Human"
                assert indexes[0][2] == "uid"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "Person"
                assert indexes[1][2] == "age"
            else:
                assert indexes[0][0] == "point"
                assert indexes[0][1] == "Person"
                assert indexes[0][2] == "age"

                assert indexes[1][0] == "point"
                assert indexes[1][1] == "Human"
                assert indexes[1][2] == "uid"


class TestMemgraphModelRegistration:
    class TestMemgraphRegisterModelClass:
        async def test_registers_model_classes(self, memgraph_client):
            class Human(NodeModel):
                ogm_config = {"labels": {"Human"}}

            class HasEmotion(RelationshipModel):
                ogm_config = {"type": "HAS_EMOTION"}

            await memgraph_client.register_models(Human, HasEmotion)

            assert len(memgraph_client._registered_models) == 2
            assert len(memgraph_client._initialized_model_hashes) == 2

        async def test_raises_on_duplicate_node_model_registration(self, memgraph_client):
            class HumanOne(NodeModel):
                ogm_config = {"labels": {"Human"}}

            class HumanTwo(NodeModel):
                ogm_config = {"labels": {"Human"}}

            with pytest.raises(DuplicateModelError):
                await memgraph_client.register_models(HumanOne, HumanTwo)

        async def test_raises_on_duplicate_multi_label_node_model_registration(self, memgraph_client):
            class HumanOne(NodeModel):
                ogm_config = {"labels": {"Human", "Special"}}

            class HumanTwo(NodeModel):
                ogm_config = {"labels": {"Human", "Special"}}

            with pytest.raises(DuplicateModelError):
                await memgraph_client.register_models(HumanOne, HumanTwo)

        async def test_raises_on_duplicate_relationship_model_registration(self, memgraph_client):
            class HasEmotionOne(RelationshipModel):
                ogm_config = {"type": "HAS_EMOTION"}

            class HasEmotionTwo(RelationshipModel):
                ogm_config = {"type": "HAS_EMOTION"}

            with pytest.raises(DuplicateModelError):
                await memgraph_client.register_models(HasEmotionOne, HasEmotionTwo)

        async def test_ignores_non_model_classes(self, memgraph_client):
            class NotAModel:
                pass

            await memgraph_client.register_models(NotAModel)

            assert len(memgraph_client._registered_models) == 0

    class TestMemgraphRegisterModelDirectory:
        async def test_registers_models_from_dir(self, memgraph_client):
            await memgraph_client.register_models_directory(
                path.join(path.dirname(__file__), "..", "model_imports", "valid")
            )

            assert len(memgraph_client._registered_models) == 5
            assert len(memgraph_client._initialized_model_hashes) == 5

        async def test_ignores_non_python_files(self, memgraph_client):
            await memgraph_client.register_models_directory(
                path.join(path.dirname(__file__), "..", "model_imports", "invalid_file_type")
            )

            assert len(memgraph_client._registered_models) == 0
            assert len(memgraph_client._initialized_model_hashes) == 0

        async def test_raises_import_error_on_no_spec_returned(self, memgraph_client):
            with patch("importlib.util.spec_from_file_location", return_value=None):
                with pytest.raises(ImportError):
                    await memgraph_client.register_models_directory(
                        path.join(path.dirname(__file__), "..", "model_imports", "valid")
                    )

        async def test_raises_import_error_on_no_spec_loader_returned(self, memgraph_client):
            mock_spec = MagicMock()
            mock_spec.loader = None

            with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
                with pytest.raises(ImportError):
                    await memgraph_client.register_models_directory(
                        path.join(path.dirname(__file__), "..", "model_imports", "valid")
                    )

        async def test_raises_on_duplicate_model_identifier(self, memgraph_client):
            with pytest.raises(DuplicateModelError):
                await memgraph_client.register_models_directory(
                    path.join(path.dirname(__file__), "..", "model_imports", "invalid_duplicate_model")
                )

            assert len(memgraph_client._registered_models) == 1
            assert len(memgraph_client._initialized_model_hashes) == 0
