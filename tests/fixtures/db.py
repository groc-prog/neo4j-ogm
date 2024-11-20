"""
Fixtures for interactions with databases.
"""

from enum import Enum

import pytest
from neo4j import AsyncGraphDatabase


class ConnectionString(Enum):
    NEO4J = "bolt://localhost:7687"
    MEMGRAPH = "bolt://localhost:7688"


class Authentication(Enum):
    NEO4J = ("neo4j", "password")
    MEMGRAPH = ("memgraph", "password")


@pytest.fixture
async def neo4j_session():
    """
    Provides a session for a Neo4j database started using docker-compose.yml. The database is cleared
    after a test is done, meaning all nodes, constraints and indexes are dropped.

    Yields:
        session (AsyncSession): A session to interaction with the database.
    """
    driver = AsyncGraphDatabase.driver(uri=ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    async with driver.session() as session:
        yield session

    # Clear all nodes created during the test
    await session.run("MATCH (n) DETACH DELETE n")

    # Drop all constraints
    query_result = await session.run("SHOW CONSTRAINTS")
    constraints = [list(result.values()) async for result in query_result]

    for constraint in constraints:
        await session.run(f"DROP CONSTRAINT {constraint[1]}")  # type: ignore

    # Drop all indexes
    query_result = await session.run("SHOW INDEXES")
    indexes = [list(result.values()) async for result in query_result]

    for index in indexes:
        await session.run((f"DROP INDEX {index[1]}"))  # type: ignore

    await driver.close()
