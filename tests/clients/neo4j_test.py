# pylint: disable=missing-class-docstring, redefined-outer-name, unused-import, unused-argument

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import neo4j
import neo4j.graph
import pytest
from neo4j.exceptions import AuthError, ClientError
from typing_extensions import Annotated

from pyneo4j_ogm.clients.neo4j import Neo4jClient, ensure_neo4j_version
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    UnsupportedDatabaseVersionError,
)
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
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.neo4j import Neo4jIndexType
from tests.fixtures.db import (
    Authentication,
    ConnectionString,
    neo4j_client,
    neo4j_session,
)


async def setup_constraints(session: neo4j.AsyncSession):
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
    await query.consume()

    assert len(constraints) == 4


async def check_no_constraints(session: neo4j.AsyncSession):
    query = await session.run("SHOW CONSTRAINT")
    constraints = await query.values()
    await query.consume()

    assert len(constraints) == 0


async def setup_indexes(session: neo4j.AsyncSession):
    await session.run(
        "CREATE INDEX node_range_index FOR (n:LabelName) ON (n.propertyName_1, n.propertyName_2, n.propertyName_n)"
    )
    await session.run(
        "CREATE INDEX rel_range_index FOR ()-[r:TYPE_NAME]-() ON (r.propertyName_1, r.propertyName_2, r.propertyName_n)"
    )
    await session.run("CREATE TEXT INDEX node_text_index FOR (n:LabelName) ON (n.propertyName_1)")
    await session.run("CREATE TEXT INDEX rel_text_index FOR ()-[r:TYPE_NAME]-() ON (r.propertyName_1)")
    await session.run("CREATE POINT INDEX node_point_index FOR (n:LabelName) ON (n.propertyName_1)")
    await session.run("CREATE POINT INDEX rel_point_index FOR ()-[r:TYPE_NAME]-() ON (r.propertyName_1)")

    query = await session.run("SHOW INDEXES")
    constraints = await query.values()
    await query.consume()

    assert len(constraints) == 6


async def check_no_indexes(session: neo4j.AsyncSession):
    query = await session.run("SHOW INDEXES")
    constraints = await query.values()
    await query.consume()

    assert len(constraints) == 0


class TestNeo4jVersionDecorator:
    async def test_version_is_none(self):
        @ensure_neo4j_version(1, 1, 1)
        async def decorated_method(*args, **kwargs):
            pass

        with pytest.raises(ClientNotInitializedError):
            mocked_client = SimpleNamespace()
            await decorated_method(mocked_client)


class TestNeo4jConnection:
    async def test_successful_connect(self):
        client = Neo4jClient()
        await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    async def test_connect_is_chainable(self):
        client = Neo4jClient()
        return_value = await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

        assert client == return_value

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
    class TestNeo4jUniquenessConstraint:
        async def test_uniqueness_constraint_is_chainable(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            return_value = await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id")
            assert neo4j_client == return_value

        async def test_node_uniqueness_constraint(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id")

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "node_constraint"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["id"]

        async def test_node_uniqueness_constraint_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", ["id", "age"])

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "node_constraint"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["id", "age"]

        async def test_node_uniqueness_constraint_raise_on_existing(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id", True)

            with pytest.raises(ClientError):
                await neo4j_client.uniqueness_constraint("node_constraint", EntityType.NODE, "Person", "id", True)

        async def test_relationship_uniqueness_constraint(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint("relationship_constraint", EntityType.RELATIONSHIP, "Person", "id")

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "relationship_constraint"
            assert constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
            assert constraints[0][3] == "RELATIONSHIP"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["id"]

        async def test_relationship_uniqueness_constraint_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint(
                "relationship_constraint", EntityType.RELATIONSHIP, "Person", ["id", "age"]
            )

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "relationship_constraint"
            assert constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
            assert constraints[0][3] == "RELATIONSHIP"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["id", "age"]

        async def test_relationship_uniqueness_constraint_raise_on_existing(self, neo4j_session, neo4j_client):
            await check_no_constraints(neo4j_session)
            await neo4j_client.uniqueness_constraint(
                "relationship_constraint", EntityType.RELATIONSHIP, "Person", "id", True
            )

            with pytest.raises(ClientError):
                await neo4j_client.uniqueness_constraint(
                    "relationship_constraint", EntityType.RELATIONSHIP, "Person", "id", True
                )


class TestNeo4jIndexes:
    class TestNeo4jRangeIndex:
        async def test_node_range_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.range_index("range_index", EntityType.NODE, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "range_index"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_node_range_index_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.range_index("range_index", EntityType.NODE, "Person", ["age", "id"])

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "range_index"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age", "id"]

        async def test_relationship_range_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.range_index("range_index", EntityType.RELATIONSHIP, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "range_index"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_range_index_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.range_index("range_index", EntityType.RELATIONSHIP, "Person", ["age", "id"])

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "range_index"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age", "id"]

        async def test_range_index_is_chainable(self, neo4j_session, neo4j_client):
            return_value = await neo4j_client.range_index(
                "range_index", EntityType.RELATIONSHIP, "Person", ["age", "id"]
            )
            assert neo4j_client == return_value

        async def test_raises_on_existing_range_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.range_index("range_index", EntityType.RELATIONSHIP, "Person", ["age", "id"])

            with pytest.raises(ClientError):
                await neo4j_client.range_index("range_index", EntityType.RELATIONSHIP, "Person", ["age", "id"], True)

    class TestNeo4jTextIndex:
        async def test_node_text_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.text_index("text_index", EntityType.NODE, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "text_index"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_text_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.text_index("text_index", EntityType.RELATIONSHIP, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "text_index"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_text_index_is_chainable(self, neo4j_session, neo4j_client):
            return_value = await neo4j_client.text_index("text_index", EntityType.RELATIONSHIP, "Person", "age")
            assert neo4j_client == return_value

        async def test_raises_on_existing_text_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.text_index("text_index", EntityType.RELATIONSHIP, "Person", "age")

            with pytest.raises(ClientError):
                await neo4j_client.text_index("text_index", EntityType.RELATIONSHIP, "Person", "age", True)

    class TestNeo4jPointIndex:
        async def test_node_point_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.point_index("point_index", EntityType.NODE, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "point_index"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_point_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.point_index("point_index", EntityType.RELATIONSHIP, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "point_index"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_point_index_is_chainable(self, neo4j_session, neo4j_client):
            return_value = await neo4j_client.point_index("point_index", EntityType.RELATIONSHIP, "Person", "age")
            assert neo4j_client == return_value

        async def test_raises_on_existing_point_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.point_index("point_index", EntityType.RELATIONSHIP, "Person", "age")

            with pytest.raises(ClientError):
                await neo4j_client.point_index("point_index", EntityType.RELATIONSHIP, "Person", "age", True)

    class TestNeo4jFulltextIndex:
        async def test_node_fulltext_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.NODE, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_node_fulltext_index_multiple_labels(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.NODE, ["Person", "Worker"], "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person", "Worker"]
            assert indexes[0][7] == ["age"]

        async def test_node_fulltext_index_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.NODE, "Person", ["age", "name"])

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age", "name"]

        async def test_relationship_fulltext_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_fulltext_index_multiple_types(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, ["Person", "Worker"], "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person", "Worker"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_fulltext_index_multiple_properties(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, "Person", ["age", "name"])

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "fulltext_index"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age", "name"]

        async def test_fulltext_index_is_chainable(self, neo4j_session, neo4j_client):
            return_value = await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, "Person", "age")
            assert neo4j_client == return_value

        async def test_raises_on_existing_fulltext_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, "Person", "age")

            with pytest.raises(ClientError):
                await neo4j_client.fulltext_index("fulltext_index", EntityType.RELATIONSHIP, "Person", "age", True)

    class TestNeo4jVectorIndex:
        async def test_node_vector_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.vector_index("vector_index", EntityType.NODE, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "vector_index"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_relationship_vector_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.vector_index("vector_index", EntityType.RELATIONSHIP, "Person", "age")

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert indexes[0][1] == "vector_index"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]

        async def test_vector_index_is_chainable(self, neo4j_session, neo4j_client):
            return_value = await neo4j_client.vector_index("vector_index", EntityType.RELATIONSHIP, "Person", "age")
            assert neo4j_client == return_value

        async def test_raises_on_existing_vector_index(self, neo4j_session, neo4j_client):
            await check_no_indexes(neo4j_session)
            await neo4j_client.vector_index("vector_index", EntityType.RELATIONSHIP, "Person", "age")

            with pytest.raises(ClientError):
                await neo4j_client.vector_index("vector_index", EntityType.RELATIONSHIP, "Person", "age", True)

        async def test_vector_index_min_neo4j_version(self, neo4j_session, neo4j_client):
            with patch.object(neo4j_client, "_version", "4.28.9"):
                with pytest.raises(UnsupportedDatabaseVersionError):
                    await neo4j_client.vector_index("vector_index", EntityType.RELATIONSHIP, "Person", "age")

            with patch.object(neo4j_client, "_version", "5.1.9"):
                with pytest.raises(UnsupportedDatabaseVersionError):
                    await neo4j_client.vector_index("vector_index", EntityType.NODE, "Person", "age")


class TestNeo4jQueries:
    async def test_drop_constraints(self, neo4j_session, neo4j_client):
        await setup_constraints(neo4j_session)
        return_value = await neo4j_client.drop_constraints()
        assert neo4j_client == return_value

        await check_no_constraints(neo4j_session)

    async def test_drop_constraints_is_chainable(self, neo4j_session, neo4j_client):
        return_value = await neo4j_client.drop_constraints()
        assert neo4j_client == return_value

    async def test_does_nothing_if_no_constraints_defined(self, neo4j_session, neo4j_client):
        with patch("pyneo4j_ogm.clients.neo4j.Neo4jClient.cypher", wraps=neo4j_client.cypher) as cypher_spy:
            await neo4j_client.drop_constraints()

            cypher_spy.assert_called_once()

    async def test_drop_indexes(self, neo4j_session, neo4j_client):
        await setup_indexes(neo4j_session)
        await neo4j_client.drop_indexes()

        await check_no_indexes(neo4j_session)

    async def test_drop_indexes_is_chainable(self, neo4j_session, neo4j_client):
        return_value = await neo4j_client.drop_indexes()
        assert neo4j_client == return_value

    async def test_does_nothing_if_no_indexes_defined(self, neo4j_session, neo4j_client):
        with patch("pyneo4j_ogm.clients.neo4j.Neo4jClient.cypher", wraps=neo4j_client.cypher) as cypher_spy:
            await neo4j_client.drop_indexes()

            cypher_spy.assert_called_once()

    async def test_drop_nodes(self, neo4j_client, neo4j_session):
        await neo4j_session.run("CREATE (:Person), (:Worker), (:People)-[:LOVES]->(:Coffee)")
        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()
        assert len(result) == 4

        await neo4j_client.drop_nodes()

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()
        assert len(result) == 0

    async def test_batching(self, neo4j_client, neo4j_session):
        async with neo4j_client.batching():
            await neo4j_client.cypher("CREATE (:Developer)")
            await neo4j_client.cypher("CREATE (:Coffee)")
            await neo4j_client.cypher("MATCH (n:Developer), (m:Coffee) CREATE (n)-[:LOVES]->(m)")

            query = await neo4j_session.run("MATCH (n) RETURN n")
            result = await query.values()
            await query.consume()
            assert len(result) == 0

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()
        assert len(result) == 2

    async def test_cypher(self, neo4j_client, neo4j_session):
        labels = ["Developer", "Coffee"]
        result, keys = await neo4j_client.cypher(f"CREATE (n:{labels[0]}), (m:{labels[1]})")

        assert len(result) == 0
        assert len(keys) == 0

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()

        assert len(result) == 2
        assert len(result[0][0].labels) == 1
        assert len(result[0][0].labels) == 1
        assert list(result[1][0].labels)[0] in labels
        assert list(result[1][0].labels)[0] in labels

    async def test_cypher_with_params(self, neo4j_client, neo4j_session):
        result, keys = await neo4j_client.cypher("CREATE (n:Person) SET n.age = $age", {"age": 24})

        assert len(result) == 0
        assert len(keys) == 0

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()

        assert len(result) == 1
        assert len(result[0][0].labels) == 1
        assert list(result[0][0].labels)[0] == "Person"
        assert dict(result[0][0])["age"] == 24

    async def test_cypher_auto_committing(self, neo4j_client, neo4j_session):
        labels = ["Developer", "Coffee"]
        result, keys = await neo4j_client.cypher(f"CREATE (n:{labels[0]}), (m:{labels[1]})", auto_committing=True)

        assert len(result) == 0
        assert len(keys) == 0

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()

        assert len(result) == 2
        assert len(result[0][0].labels) == 1
        assert len(result[0][0].labels) == 1
        assert list(result[1][0].labels)[0] in labels
        assert list(result[1][0].labels)[0] in labels

    async def test_cypher_with_params_auto_committing(self, neo4j_client, neo4j_session):
        result, keys = await neo4j_client.cypher(
            "CREATE (n:Person) SET n.age = $age", {"age": 24}, auto_committing=True
        )

        assert len(result) == 0
        assert len(keys) == 0

        query = await neo4j_session.run("MATCH (n) RETURN n")
        result = await query.values()
        await query.consume()

        assert len(result) == 1
        assert len(result[0][0].labels) == 1
        assert list(result[0][0].labels)[0] == "Person"
        assert dict(result[0][0])["age"] == 24


class TestNeo4jModelInitialization:
    async def test_skips_initialization_if_all_client_skips_defined(self, neo4j_session):
        class Person(NodeModel):
            uid: Annotated[str, UniquenessConstraint(), RangeIndex()]

        class Likes(RelationshipModel):
            uid: Annotated[str, UniquenessConstraint(), RangeIndex()]

        client = await Neo4jClient().connect(
            ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_constraints=True, skip_indexes=True
        )
        await client.register_models(Person, Likes)

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()
        await query.consume()

        assert len(constraints) == 0

        query = await neo4j_session.run("SHOW INDEXES")
        indexes = await query.values()
        await query.consume()

        assert len(indexes) == 0

    async def test_skips_initialization_if_all_model_skips_defined(self, neo4j_session):
        class Person(NodeModel):
            uid: Annotated[str, UniquenessConstraint(), RangeIndex()]

            ogm_config = {"skip_constraint_creation": True, "skip_index_creation": True}

        class Likes(RelationshipModel):
            uid: Annotated[str, UniquenessConstraint(), RangeIndex()]

            ogm_config = {"skip_constraint_creation": True, "skip_index_creation": True}

        client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
        await client.register_models(Person, Likes)

        query = await neo4j_session.run("SHOW CONSTRAINT")
        constraints = await query.values()
        await query.consume()

        assert len(constraints) == 0

        query = await neo4j_session.run("SHOW INDEXES")
        indexes = await query.values()
        await query.consume()

        assert len(indexes) == 0

    class TestNeo4jUniquenessConstraint:
        async def test_model_registration_with_client_skipped_constraints(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_constraints=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_model_skipped_constraints(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"skip_constraint_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_uniqueness_constraint(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

            class Likes(RelationshipModel):
                uid: Annotated[str, UniquenessConstraint()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2
            assert constraints[0][1] == "CT_UniquenessConstraint_LIKES_uid"
            assert constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
            assert constraints[0][3] == "RELATIONSHIP"
            assert constraints[0][4] == ["LIKES"]
            assert constraints[0][5] == ["uid"]
            assert constraints[1][1] == "CT_UniquenessConstraint_Person_uid"
            assert constraints[1][2] == "UNIQUENESS"
            assert constraints[1][3] == "NODE"
            assert constraints[1][4] == ["Person"]
            assert constraints[1][5] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "CT_UniquenessConstraint_Person_uid"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "CT_UniquenessConstraint_Human_uid"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Human"]
            assert constraints[0][5] == ["uid"]

        async def test_model_registration_with_uniqueness_constraint_invalid_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_uniqueness_constraint(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint()]
                age: Annotated[int, UniquenessConstraint()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2
            assert constraints[0][1] == "CT_UniquenessConstraint_Person_age"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["age"]
            assert constraints[1][1] == "CT_UniquenessConstraint_Person_uid"
            assert constraints[1][2] == "UNIQUENESS"
            assert constraints[1][3] == "NODE"
            assert constraints[1][4] == ["Person"]
            assert constraints[1][5] == ["uid"]

        async def test_model_registration_with_multiple_uniqueness_constraint_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human")]
                age: Annotated[int, UniquenessConstraint(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 2
            assert constraints[0][1] == "CT_UniquenessConstraint_Human_uid"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Human"]
            assert constraints[0][5] == ["uid"]
            assert constraints[1][1] == "CT_UniquenessConstraint_Person_age"
            assert constraints[1][2] == "UNIQUENESS"
            assert constraints[1][3] == "NODE"
            assert constraints[1][4] == ["Person"]
            assert constraints[1][5] == ["age"]

        async def test_model_registration_with_multiple_uniqueness_constraint_invalid_composite_key(
            self, neo4j_session
        ):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(specified_label="Human", composite_key="key")]
                age: Annotated[int, UniquenessConstraint(specified_label="Person", composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 0

        async def test_model_registration_with_multiple_uniqueness_constraint_composite_key(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, UniquenessConstraint(composite_key="key")]
                age: Annotated[int, UniquenessConstraint(composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            constraints = await query.values()
            await query.consume()

            assert len(constraints) == 1
            assert constraints[0][1] == "CT_UniquenessConstraint_Person_uid_age"
            assert constraints[0][2] == "UNIQUENESS"
            assert constraints[0][3] == "NODE"
            assert constraints[0][4] == ["Person"]
            assert constraints[0][5] == ["uid", "age"]

    class TestNeo4jRangeIndex:
        async def test_model_registration_with_client_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, RangeIndex()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_indexes=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, RangeIndex()]

                ogm_config = {"skip_index_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_range_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, RangeIndex()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_RangeIndex_LIKES_uid"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["LIKES"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_RangeIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.RANGE.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_range_index_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_RangeIndex_Person_uid"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_range_index_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_RangeIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_range_index_invalid_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_range_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex()]
                age: Annotated[int, RangeIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_RangeIndex_Person_age"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]
            assert indexes[1][1] == "IX_RangeIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.RANGE.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_multiple_range_index_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex(specified_label="Human")]
                age: Annotated[int, RangeIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_RangeIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_RangeIndex_Person_age"
            assert indexes[1][4] == Neo4jIndexType.RANGE.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["age"]

        async def test_model_registration_with_multiple_range_index_invalid_composite_key(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex(specified_label="Human", composite_key="key")]
                age: Annotated[int, RangeIndex(specified_label="Person", composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_range_index_composite_key(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, RangeIndex(composite_key="key")]
                age: Annotated[int, RangeIndex(composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert len(indexes) == 1
            assert indexes[0][1] == "IX_RangeIndex_Person_uid_age"
            assert indexes[0][4] == Neo4jIndexType.RANGE.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["uid", "age"]

    class TestNeo4jTextIndex:
        async def test_model_registration_with_client_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, TextIndex()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_indexes=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, TextIndex()]

                ogm_config = {"skip_index_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_text_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, TextIndex()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_TextIndex_LIKES_uid"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["LIKES"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_TextIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.TEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_text_index_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_TextIndex_Person_uid"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_text_index_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_TextIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_text_index_invalid_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_text_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex()]
                age: Annotated[int, TextIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_TextIndex_Person_age"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]
            assert indexes[1][1] == "IX_TextIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.TEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_multiple_text_index_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, TextIndex(specified_label="Human")]
                age: Annotated[int, TextIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_TextIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.TEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_TextIndex_Person_age"
            assert indexes[1][4] == Neo4jIndexType.TEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["age"]

    class TestNeo4jVectorIndex:
        async def test_model_registration_with_client_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, VectorIndex()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_indexes=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, VectorIndex()]

                ogm_config = {"skip_index_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_vector_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, VectorIndex()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_VectorIndex_LIKES_uid"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["LIKES"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_VectorIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.VECTOR.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_vector_index_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_VectorIndex_Person_uid"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_vector_index_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_VectorIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_vector_index_invalid_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_vector_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex()]
                age: Annotated[int, VectorIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_VectorIndex_Person_age"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]
            assert indexes[1][1] == "IX_VectorIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.VECTOR.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_multiple_vector_index_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, VectorIndex(specified_label="Human")]
                age: Annotated[int, VectorIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_VectorIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.VECTOR.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_VectorIndex_Person_age"
            assert indexes[1][4] == Neo4jIndexType.VECTOR.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["age"]

    class TestNeo4jPointIndex:
        async def test_model_registration_with_client_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_indexes=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"skip_index_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"skip_index_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_vector_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, PointIndex()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_PointIndex_LIKES_uid"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["LIKES"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_PointIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.POINT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_vector_index_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_PointIndex_Person_uid"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_vector_index_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_PointIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_vector_index_invalid_specified_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_vector_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex()]
                age: Annotated[int, PointIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_PointIndex_Person_age"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person"]
            assert indexes[0][7] == ["age"]
            assert indexes[1][1] == "IX_PointIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.POINT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_multiple_vector_index_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, PointIndex(specified_label="Human")]
                age: Annotated[int, PointIndex(specified_label="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_PointIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.POINT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_PointIndex_Person_age"
            assert indexes[1][4] == Neo4jIndexType.POINT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["age"]

    class TestNeo4jFullTextIndex:
        async def test_model_registration_with_client_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, FullTextIndex()]

            client = await Neo4jClient().connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, skip_indexes=True
            )
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_model_skipped_indexes(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex()]

                ogm_config = {"skip_constraint_creation": True}

            class Likes(RelationshipModel):
                uid: Annotated[str, FullTextIndex()]

                ogm_config = {"skip_constraint_creation": True}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW CONSTRAINT")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_full_text_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex()]

            class Likes(RelationshipModel):
                uid: Annotated[str, FullTextIndex()]

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person, Likes)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[1][1] == "IX_FullTextIndex_Person_uid"
            assert indexes[1][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["uid"]
            assert indexes[0][1] == "IX_FullTextIndex_LIKES_uid"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.RELATIONSHIP.value
            assert indexes[0][6] == ["LIKES"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_full_text_index_multi_label(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_FullTextIndex_Person_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person", "Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_full_text_index_specified_labels_as_str(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(specified_labels="Human")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_FullTextIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_full_text_index_specified_labels_as_list(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(specified_labels=["Human"])]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_FullTextIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]

        async def test_model_registration_with_full_text_index_invalid_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(specified_labels="Foo")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_full_text_index(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex()]
                age: Annotated[int, FullTextIndex()]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_FullTextIndex_Person_Human_age"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person", "Human"]
            assert indexes[0][7] == ["age"]
            assert indexes[1][1] == "IX_FullTextIndex_Person_Human_uid"
            assert indexes[1][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person", "Human"]
            assert indexes[1][7] == ["uid"]

        async def test_model_registration_with_multiple_full_text_index_specified_labels(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(specified_labels="Human")]
                age: Annotated[int, FullTextIndex(specified_labels="Person")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 2
            assert indexes[0][1] == "IX_FullTextIndex_Human_uid"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Human"]
            assert indexes[0][7] == ["uid"]
            assert indexes[1][1] == "IX_FullTextIndex_Person_age"
            assert indexes[1][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[1][5] == EntityType.NODE.value
            assert indexes[1][6] == ["Person"]
            assert indexes[1][7] == ["age"]

        async def test_model_registration_with_multiple_full_text_index_invalid_composite_key(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(specified_labels="Human", composite_key="key")]
                age: Annotated[int, FullTextIndex(specified_labels="Person", composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            with pytest.raises(ValueError):
                await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 0

        async def test_model_registration_with_multiple_full_text_index_composite_key(self, neo4j_session):
            class Person(NodeModel):
                uid: Annotated[str, FullTextIndex(composite_key="key")]
                age: Annotated[int, FullTextIndex(composite_key="key")]

                ogm_config = {"labels": ["Person", "Human"]}

            client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
            await client.register_models(Person)

            query = await neo4j_session.run("SHOW INDEXES")
            indexes = await query.values()
            await query.consume()

            assert len(indexes) == 1
            assert indexes[0][1] == "IX_FullTextIndex_Person_Human_uid_age"
            assert indexes[0][4] == Neo4jIndexType.FULLTEXT.value
            assert indexes[0][5] == EntityType.NODE.value
            assert indexes[0][6] == ["Person", "Human"]
            assert indexes[0][7] == ["uid", "age"]
