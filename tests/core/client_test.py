# pylint: disable=missing-class-docstring, redefined-outer-name, unused-import, unused-argument

from unittest.mock import AsyncMock, patch

import neo4j
import pytest
from neo4j.exceptions import AuthError, ClientError

from pyneo4j_ogm.core.client import MemgraphClient, Neo4jClient
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    UnsupportedDatabaseVersionError,
)
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.memgraph import MemgraphDataType
from tests.fixtures.db import (
    Authentication,
    ConnectionString,
    memgraph_client,
    memgraph_session,
    neo4j_client,
    neo4j_session,
)


class TestNeo4jConnection:

    async def test_successful_connect(self):
        client = Neo4jClient()
        await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    async def test_checks_connectivity_and_auth(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch(
            "neo4j.AsyncDriver.verify_connectivity", wraps=async_driver_mock.verify_connectivity
        ) as verify_connectivity_spy:
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

            verify_connectivity_spy.assert_called()

    async def test_validates_neo4j_version(self):
        server_info_mock = AsyncMock()
        server_info_mock.agent = "Neo4j/4.9.0"

        with patch("neo4j.AsyncDriver.get_server_info", return_value=server_info_mock):
            with pytest.raises(UnsupportedDatabaseVersionError):
                client = Neo4jClient()
                await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    async def test_raises_on_invalid_uri(self):
        with pytest.raises(ValueError):
            client = Neo4jClient()
            await client.connect("bolt://invalid-connection:9999")

    async def test_raises_on_invalid_auth(self):
        with pytest.raises(AuthError):
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value)

    async def test_is_chainable(self):
        client = Neo4jClient()
        chainable = await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

        assert isinstance(chainable, Neo4jClient)
        assert chainable == client

    async def test_connected_state(self):
        client = Neo4jClient()

        is_connected = await client.connected()
        assert is_connected is False

        await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
        is_connected = await client.connected()
        assert is_connected is True

    async def test_connected_on_connection_failure(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch(
            "neo4j.AsyncDriver.verify_connectivity", wraps=async_driver_mock.verify_connectivity
        ) as verify_connectivity_spy:
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

            verify_connectivity_spy.side_effect = RuntimeError("Mocked failure")

            is_connected = await client.connected()
            assert is_connected is False

    async def test_close_raises_if_not_initialized(self):
        client = Neo4jClient()

        with pytest.raises(ClientNotInitializedError):
            await client.close()

    async def test_closes_driver_connection(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch("neo4j.AsyncDriver.close", wraps=async_driver_mock.close) as close_spy:
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

            await client.close()
            close_spy.assert_called()
            assert client._driver is None


class TestNeo4jConstraints:
    async def _setup_constraints(self, session: neo4j.AsyncSession):
        await session.run("CREATE CONSTRAINT book_isbn FOR (book:Book) REQUIRE book.isbn IS UNIQUE")
        await session.run("CREATE CONSTRAINT sequels FOR ()-[sequel:SEQUEL_OF]-() REQUIRE sequel.order IS UNIQUE")
        await session.run(
            "CREATE CONSTRAINT book_title_year FOR (book:Book) REQUIRE (book.title, book.publicationYear) IS UNIQUE"
        )
        await session.run(
            "CREATE CONSTRAINT prequels FOR ()-[prequel:PREQUEL_OF]-() REQUIRE (prequel.order, prequel.author) IS UNIQUE"
        )

        query = await session.run("SHOW CONSTRAINT")
        constraints = await query.values()
        assert len(constraints) == 4

    async def _check_no_constraints(self, session: neo4j.AsyncSession):
        query = await session.run("SHOW CONSTRAINT")
        constraints = await query.values()
        assert len(constraints) == 0

    async def test_drop_constraints(self, neo4j_session, neo4j_client):
        await self._setup_constraints(neo4j_session)
        await neo4j_client.drop_constraints()

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()
        assert len(constraints) == 0

    async def test_does_nothing_if_no_constraints_defined(self, neo4j_session, neo4j_client):
        with patch("pyneo4j_ogm.core.client.Neo4jClient.cypher", wraps=neo4j_client.cypher) as cypher_spy:
            await neo4j_client.drop_constraints()

            cypher_spy.assert_called_once()

    async def test_node_uniqueness_constraint(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id")

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][1] == "node_constraint"
        assert constraints[0][2] == "UNIQUENESS"
        assert constraints[0][3] == "NODE"
        assert constraints[0][4] == ["Person"]
        assert constraints[0][5] == ["id"]

    async def test_node_uniqueness_constraint_multiple_properties(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", ["id", "age"])

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][1] == "node_constraint"
        assert constraints[0][2] == "UNIQUENESS"
        assert constraints[0][3] == "NODE"
        assert constraints[0][4] == ["Person"]
        assert constraints[0][5] == ["id", "age"]

    async def test_node_uniqueness_constraint_raise_on_existing(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id", True)

        with pytest.raises(ClientError):
            await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id", True)

    async def test_relationship_uniqueness_constraint(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint("relationship_constraint", EntityType.RELATIONSHIP, "Person", "id")

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][1] == "relationship_constraint"
        assert constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
        assert constraints[0][3] == "RELATIONSHIP"
        assert constraints[0][4] == ["Person"]
        assert constraints[0][5] == ["id"]

    async def test_relationship_uniqueness_constraint_multiple_properties(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint(
            "relationship_constraint", EntityType.RELATIONSHIP, "Person", ["id", "age"]
        )

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][1] == "relationship_constraint"
        assert constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
        assert constraints[0][3] == "RELATIONSHIP"
        assert constraints[0][4] == ["Person"]
        assert constraints[0][5] == ["id", "age"]

    async def test_relationship_uniqueness_constraint_raise_on_existing(self, neo4j_session, neo4j_client):
        await self._check_no_constraints(neo4j_session)
        await neo4j_client.uniqueness_constraint(
            "relationship_constraint", EntityType.RELATIONSHIP, "Person", "id", True
        )

        with pytest.raises(ClientError):
            await neo4j_client.uniqueness_constraint(
                "relationship_constraint", EntityType.RELATIONSHIP, "Person", "id", True
            )


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

    async def test_is_chainable(self):
        client = MemgraphClient()
        chainable = await client.connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)

        assert isinstance(chainable, MemgraphClient)
        assert chainable == client

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
    async def _setup_constraints(self, session: neo4j.AsyncSession):
        await session.run("CREATE CONSTRAINT ON (n:Employee) ASSERT n.id IS UNIQUE")
        await session.run("CREATE CONSTRAINT ON (n:Employee) ASSERT n.name, n.address IS UNIQUE")

        query = await session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()
        assert len(constraints) == 2

    async def _check_no_constraints(self, session: neo4j.AsyncSession):
        query = await session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()
        assert len(constraints) == 0

    async def test_drop_constraints(self, memgraph_session, memgraph_client):
        await self._setup_constraints(memgraph_session)
        await memgraph_client.drop_constraints()

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()
        assert len(constraints) == 0

    async def test_does_nothing_if_no_constraints_defined(self, memgraph_session, memgraph_client):
        with patch("pyneo4j_ogm.core.client.MemgraphClient.cypher", wraps=memgraph_client.cypher) as cypher_spy:
            await memgraph_client.drop_constraints()

            cypher_spy.assert_called_once()

    async def test_existence_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.existence_constraint("Person", "id")

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "exists"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"

    async def test_existence_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.existence_constraint("Person", ["id", "age"])

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "exists"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[1][0] == "exists"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"

    async def test_uniqueness_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.uniqueness_constraint("Person", "id")

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "unique"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == ["id"]

    async def test_uniqueness_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.uniqueness_constraint("Person", ["id", "age"])

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "unique"
        assert constraints[0][1] == "Person"
        assert set(constraints[0][2]) == set(["id", "age"])

    async def test_bool_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.BOOLEAN)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "BOOL"

    async def test_bool_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.BOOLEAN)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "BOOL"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "BOOL"

    async def test_string_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.STRING)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "STRING"

    async def test_string_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.STRING)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "STRING"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "STRING"

    async def test_int_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.INTEGER)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "INTEGER"

    async def test_int_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.INTEGER)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "INTEGER"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "INTEGER"

    async def test_float_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.FLOAT)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "FLOAT"

    async def test_float_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.FLOAT)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "FLOAT"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "FLOAT"

    async def test_list_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LIST)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LIST"

    async def test_list_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.LIST)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LIST"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "LIST"

    async def test_map_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.MAP)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "MAP"

    async def test_map_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.MAP)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "MAP"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "MAP"

    async def test_duration_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DURATION)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "DURATION"

    async def test_duration_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.DURATION)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "DURATION"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "DURATION"

    async def test_date_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.DATE)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "DATE"

    async def test_date_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.DATE)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "DATE"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "DATE"

    async def test_local_time_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_TIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LOCAL TIME"

    async def test_local_time_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.LOCAL_TIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LOCAL TIME"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "LOCAL TIME"

    async def test_local_datetime_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.LOCAL_DATETIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LOCAL DATE TIME"

    async def test_local_datetime_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.LOCAL_DATETIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "LOCAL DATE TIME"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "LOCAL DATE TIME"

    async def test_zoned_datetime_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ZONED_DATETIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "ZONED DATE TIME"

    async def test_zoned_datetime_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.ZONED_DATETIME)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "ZONED DATE TIME"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "ZONED DATE TIME"

    async def test_enum_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.ENUM)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "ENUM"

    async def test_enum_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.ENUM)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "ENUM"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "ENUM"

    async def test_point_data_type_constraint(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", "id", MemgraphDataType.POINT)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 1
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "POINT"

    async def test_point_data_type_constraint_multiple_properties(self, memgraph_session, memgraph_client):
        await self._check_no_constraints(memgraph_session)
        await memgraph_client.data_type_constraint("Person", ["id", "age"], MemgraphDataType.POINT)

        query = await memgraph_session.run("SHOW CONSTRAINT INFO")
        constraints = await query.values()

        assert len(constraints) == 2
        assert constraints[0][0] == "data_type"
        assert constraints[0][1] == "Person"
        assert constraints[0][2] == "id"
        assert constraints[0][3] == "POINT"
        assert constraints[1][0] == "data_type"
        assert constraints[1][1] == "Person"
        assert constraints[1][2] == "age"
        assert constraints[1][3] == "POINT"
