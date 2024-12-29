# pylint: disable=unused-argument, unused-import, redefined-outer-name, protected-access, missing-module-docstring, missing-class-docstring
# pyright: reportGeneralTypeIssues=false
import asyncio
import os
from typing import cast
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from neo4j import AsyncSession
from neo4j.exceptions import CypherSyntaxError
from neo4j.graph import Node, Path, Relationship

from pyneo4j_ogm.core.client import EntityType, Pyneo4jClient
from pyneo4j_ogm.core.node import NodeModel
from pyneo4j_ogm.core.relationship import RelationshipModel
from pyneo4j_ogm.exceptions import (
    InvalidEntityType,
    InvalidLabelOrType,
    MissingDatabaseURI,
    NotConnectedToDatabase,
    TransactionInProgress,
    UnsupportedNeo4jVersion,
)
from pyneo4j_ogm.fields.property_options import WithOptions
from pyneo4j_ogm.logger import logger
from tests.fixtures.db_setup import client, session
from tests.fixtures.models.models_top import ModelOne, ModelTwo
from tests.fixtures.models.nested.deeply_nested.model_deeply_nested import (
    ModelFive,
    ModelSix,
)
from tests.fixtures.models.nested.model_nested import ModelFour, ModelThree


class CypherResolvingNode(NodeModel):
    name: str

    class Settings:
        labels = {"TestNode"}


class CypherResolvingRelationship(RelationshipModel):
    kind: str

    class Settings:
        type = "TEST_RELATIONSHIP"


async def test_batch(client: Pyneo4jClient, session: AsyncSession):
    async with client.batch() as batch:
        await client.cypher(
            "CREATE (n:Node) SET n.name = $name", parameters={"name": "TestName"},
            batch_manager=batch
        )
        await client.cypher(
            "CREATE (n:Node) SET n.name = $name", parameters={"name": "TestName2"},
            batch_manager=batch
        )

    query_results = await session.run("MATCH (n) RETURN n")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == 2


async def test_concurrent_non_batch_execution(client: Pyneo4jClient, session: AsyncSession):
    coroutines_count = 3
    coroutines = []
    for i in range(coroutines_count):
        coroutines.append(
            client.cypher("CREATE (n:Node) SET n.name = $name", parameters={"name": f"TestName{i}"},)
        )
    await asyncio.gather(*coroutines)
    query_results = await session.run("MATCH (n) RETURN n")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == coroutines_count


async def test_concurrent_batch_execution(client: Pyneo4jClient, session: AsyncSession):
    coroutines_count = 3
    queries_by_batch = 3
    coroutines = []

    async def batch_query(n):
        async with client.batch() as batch:
            print(batch)
            for j in range(queries_by_batch):
                print(f"{n}{j}")
                await client.cypher(
                    "CREATE (n:Node) SET n.name = $name", parameters={"name": f"TestName{n}{j}"},
                    batch_manager=batch
                )

    for i in range(coroutines_count):
        coroutines.append(
            batch_query(i)
        )
    await asyncio.gather(*coroutines)
    query_results = await session.run("MATCH (n) RETURN n")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == coroutines_count * queries_by_batch


async def test_batch_exception(client: Pyneo4jClient, session: AsyncSession):
    with pytest.raises(Exception):
        async with client.batch() as batch:
            await client.cypher(
                "CREATE (n:Node) SET n.name = $name", parameters={"name": "TestName"},
                batch_manager=batch
            )
            await client.cypher(
                "CREATE (n:Node) SET n.name = $name", parameters={"name": "TestName"},
                batch_manager=batch
            )

            raise Exception("Test Exception")  # pylint: disable=broad-exception-raised

    query_results = await session.run("MATCH (n) RETURN n")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == 0


async def test_transaction_in_progress_exception(client: Pyneo4jClient):
    async with client.batch() as batch:
        with pytest.raises(TransactionInProgress):
            await batch._begin_transaction()


async def test_connection():
    client = await Pyneo4jClient().connect("bolt://localhost:7687", auth=("neo4j", "password"))
    assert client.is_connected
    assert client._driver is not None

    await client.close()
    assert not client.is_connected
    assert client._driver is None

    os.environ["NEO4J_OGM_URI"] = "bolt://localhost:7687"

    client = await Pyneo4jClient().connect(auth=("neo4j", "password"))
    assert client.is_connected
    assert client._driver is not None

    await client.close()
    assert not client.is_connected
    assert client._driver is None

    os.environ.pop("NEO4J_OGM_URI")

    with pytest.raises(MissingDatabaseURI):
        client = await Pyneo4jClient().connect()


async def test_ensure_connection(client: Pyneo4jClient):
    with pytest.raises(NotConnectedToDatabase):
        client = Pyneo4jClient()
        await client.cypher("MATCH (n) RETURN n")

    await client.connect("bolt://localhost:7687", auth=("neo4j", "password"))
    results, _ = await client.cypher("MATCH (n) RETURN n")
    assert results == []

    with pytest.raises(NotConnectedToDatabase):
        await client.close()
        await client.cypher("MATCH (n) RETURN n")


async def test_register_models(client: Pyneo4jClient, session: AsyncSession):
    class ClientNodeModel(NodeModel):
        a: WithOptions(str, unique=True)
        b: WithOptions(str, range_index=True)
        c: WithOptions(str, text_index=True)
        d: WithOptions(str, point_index=True)

        class Settings:
            labels = {"Test", "Node"}

    class ClientRelationshipModel(RelationshipModel):
        a: WithOptions(str, unique=True)
        b: WithOptions(str, range_index=True)
        c: WithOptions(str, text_index=True)
        d: WithOptions(str, point_index=True)

        class Settings:
            type = "TEST_RELATIONSHIP"

    await client.register_models([ClientNodeModel, ClientRelationshipModel])

    assert ClientNodeModel in client.models
    assert ClientRelationshipModel in client.models

    query_results = await session.run("SHOW CONSTRAINTS")
    constraint_results = [result.values() async for result in query_results]
    await query_results.consume()

    assert len(constraint_results) == 3

    query_results = await session.run("SHOW INDEXES")
    index_results = [result.values() async for result in query_results]
    await query_results.consume()

    assert len(index_results) == 12

    assert index_results[8][1] == "ClientRelationshipModel_TEST_RELATIONSHIP_a_unique_constraint"
    assert index_results[8][4] == "RANGE"
    assert index_results[8][5] == EntityType.RELATIONSHIP
    assert index_results[8][6] == ["TEST_RELATIONSHIP"]
    assert index_results[8][7] == ["a"]

    assert index_results[9][1] == "ClientRelationshipModel_TEST_RELATIONSHIP_b_range_index"
    assert index_results[9][4] == "RANGE"
    assert index_results[9][5] == EntityType.RELATIONSHIP
    assert index_results[9][6] == ["TEST_RELATIONSHIP"]
    assert index_results[9][7] == ["b"]

    assert index_results[10][1] == "ClientRelationshipModel_TEST_RELATIONSHIP_c_text_index"
    assert index_results[10][4] == "TEXT"
    assert index_results[10][5] == EntityType.RELATIONSHIP
    assert index_results[10][6] == ["TEST_RELATIONSHIP"]
    assert index_results[10][7] == ["c"]

    assert index_results[11][1] == "ClientRelationshipModel_TEST_RELATIONSHIP_d_point_index"
    assert index_results[11][4] == "POINT"
    assert index_results[11][5] == EntityType.RELATIONSHIP
    assert index_results[11][6] == ["TEST_RELATIONSHIP"]
    assert index_results[11][7] == ["d"]


async def test_supported_neo4j_version():
    mock_driver = MagicMock()
    mock_driver.get_server_info = AsyncMock(return_value=MagicMock(agent="Neo4j/4.0.0"))

    with patch("neo4j.AsyncGraphDatabase.driver", return_value=mock_driver):
        with pytest.raises(UnsupportedNeo4jVersion):
            await Pyneo4jClient().connect("bolt://localhost:7687", auth=("neo4j", "password"))


async def test_close():
    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()

    with patch("pyneo4j_ogm.core.client.Pyneo4jClient.is_connected", new_callable=PropertyMock) as mock_is_connected:
        mock_is_connected.return_value = False
        client = await Pyneo4jClient().connect("bolt://localhost:7687", auth=("neo4j", "password"))

        with patch("neo4j.AsyncGraphDatabase.driver", return_value=mock_driver):
            await client.close()
            mock_driver.close.assert_not_called()

    await client.close()


async def test_invalid_constraints(client: Pyneo4jClient):
    with pytest.raises(InvalidLabelOrType):
        await client.create_uniqueness_constraint(
            "invalid_node_constraint", EntityType.NODE, ["prop_a", "prop_b"], "Test"
        )

    with pytest.raises(InvalidLabelOrType):
        await client.create_uniqueness_constraint(
            "invalid_relationship_constraint", EntityType.RELATIONSHIP, ["prop_a", "prop_b"], ["Test", "Relationship"]
        )

    with pytest.raises(InvalidEntityType):
        await client.create_uniqueness_constraint(
            "invalid_constraint", "invalid entity", ["prop_a", "prop_b"], ["Test", "Relationship"]  # type: ignore
        )


async def test_create_node_constraints(client: Pyneo4jClient, session: AsyncSession):
    await client.create_uniqueness_constraint(
        "node_constraint", EntityType.NODE, ["prop_a", "prop_b"], ["Test", "Node"]
    )

    node_constraint_results = await session.run("SHOW CONSTRAINTS")
    node_constraints = await node_constraint_results.values()
    await node_constraint_results.consume()

    assert node_constraints[0][1] == "node_constraint_Node_prop_a_prop_b_unique_constraint"
    assert node_constraints[0][2] == "UNIQUENESS"
    assert node_constraints[0][3] == "NODE"
    assert node_constraints[0][4] == ["Node"]
    assert node_constraints[0][5] == ["prop_a", "prop_b"]

    assert node_constraints[1][1] == "node_constraint_Test_prop_a_prop_b_unique_constraint"
    assert node_constraints[1][2] == "UNIQUENESS"
    assert node_constraints[1][3] == "NODE"
    assert node_constraints[1][4] == ["Test"]
    assert node_constraints[1][5] == ["prop_a", "prop_b"]


async def test_create_relationship_constraints(client: Pyneo4jClient, session: AsyncSession):
    await client.create_uniqueness_constraint(
        "relationship_constraint", EntityType.RELATIONSHIP, ["prop_a", "prop_b"], "TEST_RELATIONSHIP"
    )

    node_constraint_results = await session.run("SHOW CONSTRAINTS")
    node_constraints = await node_constraint_results.values()
    await node_constraint_results.consume()

    assert node_constraints[0][1] == "relationship_constraint_TEST_RELATIONSHIP_prop_a_prop_b_unique_constraint"
    assert node_constraints[0][2] == "RELATIONSHIP_UNIQUENESS"
    assert node_constraints[0][3] == "RELATIONSHIP"
    assert node_constraints[0][4] == ["TEST_RELATIONSHIP"]
    assert node_constraints[0][5] == ["prop_a", "prop_b"]


async def test_invalid_indexes(client: Pyneo4jClient):
    with pytest.raises(InvalidEntityType):
        await client.create_range_index(
            "invalid_entity_index", "invalid entity", ["prop_a", "prop_b"], ["Test", "Node"]  # type: ignore
        )

    with pytest.raises(InvalidEntityType):
        await client.create_text_index(
            "invalid_entity_index", "invalid entity", ["prop_a", "prop_b"], ["Test", "Node"]  # type: ignore
        )

    with pytest.raises(InvalidEntityType):
        await client.create_point_index(
            "invalid_entity_index", "invalid entity", ["prop_a", "prop_b"], ["Test", "Node"]  # type: ignore
        )

    with pytest.raises(InvalidEntityType):
        await client.create_lookup_index("invalid_entity_index", "invalid entity")  # type: ignore

    with pytest.raises(InvalidLabelOrType):
        await client.create_range_index("invalid_node_index", EntityType.NODE, [], "Test")

    with pytest.raises(InvalidLabelOrType):
        await client.create_range_index(
            "invalid_relationship_index", EntityType.RELATIONSHIP, [], ["Test", "Relationship"]
        )

    with pytest.raises(InvalidLabelOrType):
        await client.create_text_index("invalid_node_index", EntityType.NODE, [], "Test")

    with pytest.raises(InvalidLabelOrType):
        await client.create_text_index(
            "invalid_relationship_index", EntityType.RELATIONSHIP, [], ["Test", "Relationship"]
        )

    with pytest.raises(InvalidLabelOrType):
        await client.create_point_index("invalid_node_index", EntityType.NODE, [], "Test")

    with pytest.raises(InvalidLabelOrType):
        await client.create_point_index(
            "invalid_relationship_index", EntityType.RELATIONSHIP, [], ["Test", "Relationship"]
        )


async def test_create_node_range_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_range_index("node_range_index", EntityType.NODE, ["prop_a", "prop_b"], ["Test", "Node"])

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "node_range_index_Node_prop_a_prop_b_range_index"
    assert index_results[0][4] == "RANGE"
    assert index_results[0][5] == EntityType.NODE
    assert index_results[0][6] == ["Node"]
    assert index_results[0][7] == ["prop_a", "prop_b"]

    assert index_results[1][1] == "node_range_index_Test_prop_a_prop_b_range_index"
    assert index_results[1][4] == "RANGE"
    assert index_results[1][5] == EntityType.NODE
    assert index_results[1][6] == ["Test"]
    assert index_results[1][7] == ["prop_a", "prop_b"]


async def test_create_relationship_range_indexes(client: Pyneo4jClient, session: AsyncSession):
    with pytest.raises(InvalidLabelOrType):
        await client.create_range_index(
            "invalid_relationship_index",
            EntityType.RELATIONSHIP,
            ["prop_a", "prop_b"],
            ["NotA", "String"],
        )

    await client.create_range_index("relationship_range_index", EntityType.RELATIONSHIP, ["prop_a", "prop_b"], "REL")

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "relationship_range_index_REL_prop_a_prop_b_range_index"
    assert index_results[0][4] == "RANGE"
    assert index_results[0][5] == EntityType.RELATIONSHIP
    assert index_results[0][6] == ["REL"]
    assert index_results[0][7] == ["prop_a", "prop_b"]


async def test_create_node_text_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_text_index("node_text_index", EntityType.NODE, ["prop_a", "prop_b"], ["Test", "Node"])

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "node_text_index_Node_prop_a_text_index"
    assert index_results[0][4] == "TEXT"
    assert index_results[0][5] == EntityType.NODE
    assert index_results[0][6] == ["Node"]
    assert index_results[0][7] == ["prop_a"]

    assert index_results[1][1] == "node_text_index_Node_prop_b_text_index"
    assert index_results[1][4] == "TEXT"
    assert index_results[1][5] == EntityType.NODE
    assert index_results[1][6] == ["Node"]
    assert index_results[1][7] == ["prop_b"]

    assert index_results[2][1] == "node_text_index_Test_prop_a_text_index"
    assert index_results[2][4] == "TEXT"
    assert index_results[2][5] == EntityType.NODE
    assert index_results[2][6] == ["Test"]
    assert index_results[2][7] == ["prop_a"]

    assert index_results[3][1] == "node_text_index_Test_prop_b_text_index"
    assert index_results[3][4] == "TEXT"
    assert index_results[3][5] == EntityType.NODE
    assert index_results[3][6] == ["Test"]
    assert index_results[3][7] == ["prop_b"]


async def test_create_relationship_text_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_text_index("relationship_text_index", EntityType.RELATIONSHIP, ["prop_a", "prop_b"], "REL")

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "relationship_text_index_REL_prop_a_text_index"
    assert index_results[0][4] == "TEXT"
    assert index_results[0][5] == EntityType.RELATIONSHIP
    assert index_results[0][6] == ["REL"]
    assert index_results[0][7] == ["prop_a"]

    assert index_results[1][1] == "relationship_text_index_REL_prop_b_text_index"
    assert index_results[1][4] == "TEXT"
    assert index_results[1][5] == EntityType.RELATIONSHIP
    assert index_results[1][6] == ["REL"]
    assert index_results[1][7] == ["prop_b"]


async def test_create_node_lookup_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_lookup_index("node_lookup_index", EntityType.NODE)

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "node_lookup_index_lookup_index"
    assert index_results[0][4] == "LOOKUP"
    assert index_results[0][5] == EntityType.NODE


async def test_create_relationship_lookup_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_lookup_index("relationship_lookup_index", EntityType.RELATIONSHIP)

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "relationship_lookup_index_lookup_index"
    assert index_results[0][4] == "LOOKUP"
    assert index_results[0][5] == EntityType.RELATIONSHIP


async def test_create_node_point_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_point_index("node_point_index", EntityType.NODE, ["prop_a", "prop_b"], ["Test", "Node"])

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "node_point_index_Node_prop_a_point_index"
    assert index_results[0][4] == "POINT"
    assert index_results[0][5] == EntityType.NODE
    assert index_results[0][6] == ["Node"]
    assert index_results[0][7] == ["prop_a"]

    assert index_results[1][1] == "node_point_index_Node_prop_b_point_index"
    assert index_results[1][4] == "POINT"
    assert index_results[1][5] == EntityType.NODE
    assert index_results[1][6] == ["Node"]
    assert index_results[1][7] == ["prop_b"]

    assert index_results[2][1] == "node_point_index_Test_prop_a_point_index"
    assert index_results[2][4] == "POINT"
    assert index_results[2][5] == EntityType.NODE
    assert index_results[2][6] == ["Test"]
    assert index_results[2][7] == ["prop_a"]

    assert index_results[3][1] == "node_point_index_Test_prop_b_point_index"
    assert index_results[3][4] == "POINT"
    assert index_results[3][5] == EntityType.NODE
    assert index_results[3][6] == ["Test"]
    assert index_results[3][7] == ["prop_b"]


async def test_create_relationship_point_indexes(client: Pyneo4jClient, session: AsyncSession):
    await client.create_point_index("relationship_point_index", EntityType.RELATIONSHIP, ["prop_a", "prop_b"], "REL")

    query_results = await session.run("SHOW INDEXES")
    index_results = await query_results.values()
    await query_results.consume()

    assert index_results[0][1] == "relationship_point_index_REL_prop_a_point_index"
    assert index_results[0][4] == "POINT"
    assert index_results[0][5] == EntityType.RELATIONSHIP
    assert index_results[0][6] == ["REL"]
    assert index_results[0][7] == ["prop_a"]

    assert index_results[1][1] == "relationship_point_index_REL_prop_b_point_index"
    assert index_results[1][4] == "POINT"
    assert index_results[1][5] == EntityType.RELATIONSHIP
    assert index_results[1][6] == ["REL"]
    assert index_results[1][7] == ["prop_b"]


async def test_cypher_query(client: Pyneo4jClient, session: AsyncSession):
    results, meta = await client.cypher("CREATE (n:Node) SET n.name = $name RETURN n", parameters={"name": "TestName"})

    assert len(results) == 1
    assert len(results[0]) == 1
    assert isinstance(results[0][0], Node)
    assert meta == ["n"]

    query_results = await session.run("MATCH (n:Node) RETURN n")
    result = await query_results.single()

    assert result is not None
    assert result["n"]["name"] == "TestName"


async def test_cypher_resolve_model_query(client: Pyneo4jClient, session: AsyncSession):
    await client.register_models([CypherResolvingNode])

    result = await session.run("CREATE (n:TestNode) SET n.name = $name", {"name": "TestName"})
    await result.consume()

    unresolved_results, _ = await client.cypher(
        "MATCH (n:TestNode) WHERE n.name = $name RETURN n", {"name": "TestName"}, resolve_models=False
    )

    assert len(unresolved_results) == 1
    assert len(unresolved_results[0]) == 1
    assert isinstance(unresolved_results[0][0], Node)

    resolved_results, _ = await client.cypher(
        "MATCH (n:TestNode) WHERE n.name = $name RETURN n", {"name": "TestName"}, resolve_models=True
    )

    assert len(resolved_results) == 1
    assert len(resolved_results[0]) == 1
    assert isinstance(resolved_results[0][0], CypherResolvingNode)


async def test_cypher_resolve_relationship_model_query(client: Pyneo4jClient, session: AsyncSession):
    await client.register_models([CypherResolvingRelationship])

    result = await session.run(
        "CREATE (:TestNode {name: $start})-[:TEST_RELATIONSHIP {kind: 'awesome'}]->(:TestNode {end: $end}) ",
        {"start": "start", "end": "end"},
    )
    await result.consume()

    unresolved_results, _ = await client.cypher(
        "MATCH ()-[r:TEST_RELATIONSHIP]->() WHERE r.kind = $kind RETURN r", {"kind": "awesome"}, resolve_models=False
    )

    assert len(unresolved_results) == 1
    assert len(unresolved_results[0]) == 1
    assert isinstance(unresolved_results[0][0], Relationship)

    resolved_results, _ = await client.cypher(
        "MATCH ()-[r:TEST_RELATIONSHIP]->() WHERE r.kind = $kind RETURN r", {"kind": "awesome"}, resolve_models=True
    )

    assert len(resolved_results) == 1
    assert len(resolved_results[0]) == 1
    assert isinstance(resolved_results[0][0], CypherResolvingRelationship)


async def test_cypher_resolve_path_query(client: Pyneo4jClient, session: AsyncSession):
    await client.register_models([CypherResolvingNode, CypherResolvingRelationship])

    result = await session.run(
        "CREATE (:TestNode {name: $start})-[:TEST_RELATIONSHIP {kind: 'awesome'}]->(:TestNode {name: $end}) ",
        {"start": "start", "end": "end"},
    )
    await result.consume()

    unresolved_results, _ = await client.cypher(
        "MATCH path = (:TestNode)-[:TEST_RELATIONSHIP]->(:TestNode) RETURN path", resolve_models=False
    )

    assert len(unresolved_results) == 1
    assert len(unresolved_results[0]) == 1
    assert isinstance(unresolved_results[0][0], Path)
    assert isinstance(cast(Path, unresolved_results[0][0]).start_node, Node)
    assert isinstance(cast(Path, unresolved_results[0][0]).end_node, Node)
    assert isinstance(cast(Path, unresolved_results[0][0]).relationships[0], Relationship)

    resolved_results, _ = await client.cypher(
        "MATCH path = (:TestNode)-[:TEST_RELATIONSHIP]->(:TestNode) RETURN path", resolve_models=True
    )

    assert len(resolved_results) == 1
    assert len(resolved_results[0]) == 1
    assert isinstance(resolved_results[0][0], Path)
    assert isinstance(cast(Path, resolved_results[0][0]).start_node, CypherResolvingNode)
    assert isinstance(cast(Path, resolved_results[0][0]).end_node, CypherResolvingNode)
    assert isinstance(cast(Path, resolved_results[0][0]).relationships[0], CypherResolvingRelationship)


async def test_cypher_query_exception(client: Pyneo4jClient):
    with pytest.raises(CypherSyntaxError):
        await client.cypher("MATCH n RETURN n", parameters={"a": 1})


async def test_drop_nodes(client: Pyneo4jClient, session: AsyncSession):
    print("BEFORE CREATE")
    result = await session.run("CREATE (n:Node) SET n.name = $name", {"name": "TestName"})
    await result.consume()

    await client.drop_nodes()
    print("DROPPED")

    query_results = await session.run("MATCH (n) RETURN n")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == 0


async def test_drop_constraints(client: Pyneo4jClient, session: AsyncSession):
    result = await session.run("CREATE CONSTRAINT test FOR (n:Node) REQUIRE n.name IS UNIQUE")
    await result.consume()

    await client.drop_constraints()

    query_results = await session.run("SHOW CONSTRAINTS")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == 0


async def test_drop_indexes(client: Pyneo4jClient, session: AsyncSession):
    result = await session.run("CREATE INDEX test FOR (n:Node) ON (n.name)")
    await result.consume()

    await client.drop_indexes()

    query_results = await session.run("SHOW INDEXES")
    results = await query_results.values()
    await query_results.consume()

    assert len(results) == 0


async def test_register_models_dir(client: Pyneo4jClient):
    await client.register_models_from_directory("tests/fixtures/models")
    assert len(client.models) == 6

    for model in client.models:
        assert getattr(model, "_client", None) is not None
