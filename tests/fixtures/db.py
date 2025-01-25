"""
Fixtures for interactions with databases.
"""

from enum import Enum

import pytest
from neo4j import AsyncGraphDatabase

from pyneo4j_ogm.clients.memgraph import MemgraphClient
from pyneo4j_ogm.clients.neo4j import Neo4jClient
from pyneo4j_ogm.types.memgraph import (
    MemgraphConstraintType,
    MemgraphDataTypeMapping,
    MemgraphIndexType,
)


class ConnectionString(Enum):
    NEO4J = "bolt://localhost:7687"
    MEMGRAPH = "bolt://localhost:7688"


class Authentication(Enum):
    NEO4J = ("neo4j", "password")
    MEMGRAPH = ("memgraph", "password")


@pytest.fixture(scope="function")
async def neo4j_client():
    """
    Provides a client for a Neo4j database started using docker-compose.yml.

    yields:
        client (Neo4jClient): A Neo4jClient instance which is already connected to the database.
    """
    client = await Neo4jClient().connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
    yield client

    await client.close()


@pytest.fixture(scope="function")
async def memgraph_client():
    """
    Provides a client for a Memgraph database started using docker-compose.yml.

    yields:
        client (MemgraphClient): A MemgraphClient instance which is already connected to the database.
    """
    client = await MemgraphClient().connect(ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
    yield client

    await client.close()


@pytest.fixture(scope="function")
async def neo4j_session():
    """
    Provides a session for a Neo4j database started using docker-compose.yml. The database is cleared
    after a test is done, meaning all nodes, constraints and indexes are dropped.

    Yields:
        session (AsyncSession): A session to interaction with the database.
    """
    driver = AsyncGraphDatabase.driver(uri=ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
    session = driver.session()

    # Clear all nodes created during the test
    query_result = await session.run("MATCH (n) DETACH DELETE n")
    await query_result.consume()

    # Drop all constraints
    query_result = await session.run("SHOW CONSTRAINTS")
    constraints = [list(result.values()) async for result in query_result]
    await query_result.consume()

    for constraint in constraints:
        await session.run(f"DROP CONSTRAINT {constraint[1]}")  # type: ignore

    # Drop all indexes
    query_result = await session.run("SHOW INDEXES")
    indexes = [list(result.values()) async for result in query_result]
    await query_result.consume()

    for index in indexes:
        await session.run((f"DROP INDEX {index[1]}"))  # type: ignore

    yield session

    await session.close()
    await driver.close()


@pytest.fixture(scope="function")
async def memgraph_session():
    """
    Provides a session for a Memgraph database started using docker-compose.yml. The database is cleared
    after a test is done, meaning all nodes, constraints and indexes are dropped.

    Yields:
        session (AsyncSession): A session to interaction with the database.
    """
    driver = AsyncGraphDatabase.driver(uri=ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value)
    session = driver.session()

    # Clear all nodes created during the test
    await session.run("MATCH (n) DETACH DELETE n")

    # Drop all constraints
    query_result = await session.run("SHOW CONSTRAINT INFO")
    constraints = [list(result.values()) async for result in query_result]
    await query_result.consume()

    for constraint in constraints:
        match constraint[0]:
            case MemgraphConstraintType.EXISTS.value:
                await session.run(
                    f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT EXISTS (n.{constraint[2]})",  # type: ignore
                )
            case MemgraphConstraintType.UNIQUE.value:
                await session.run(
                    f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT {', '.join([f'n.{constraint_property}' for constraint_property in constraint[2]])} IS UNIQUE",  # type: ignore
                )
            case MemgraphConstraintType.DATA_TYPE.value:
                # Some data types in Memgraph are returned differently that what is used when creating them
                # Because of that we have to do some additional mapping when dropping them
                await session.run(
                    f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT n.{constraint[2]} IS TYPED {MemgraphDataTypeMapping[constraint[3]]}",  # type: ignore
                )

    # Drop all indexes
    query_result = await session.run("SHOW INDEX INFO")
    indexes = [list(result.values()) async for result in query_result]
    await query_result.consume()

    for index in indexes:
        for index in indexes:
            match index[0]:
                case MemgraphIndexType.EDGE_TYPE.value:
                    await session.run(f"DROP EDGE INDEX ON :{index[1]}")  # type: ignore
                case MemgraphIndexType.EDGE_TYPE_AND_PROPERTY.value:
                    await session.run(f"DROP EDGE INDEX ON :{index[1]}({index[2]})")  # type: ignore
                case MemgraphIndexType.LABEL.value:
                    await session.run(f"DROP INDEX ON :{index[1]}")  # type: ignore
                case MemgraphIndexType.LABEL_AND_PROPERTY.value:
                    await session.run(f"DROP INDEX ON :{index[1]}({index[2]})")  # type: ignore
                case MemgraphIndexType.POINT.value:
                    await session.run(f"DROP POINT INDEX ON :{index[1]}({index[2]})")  # type: ignore

    yield session

    await session.close()
    await driver.close()
