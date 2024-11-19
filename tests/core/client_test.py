# pylint: disable=missing-class-docstring, redefined-outer-name, unnecessary-lambda

from unittest.mock import AsyncMock, patch

import neo4j
import pytest
from neo4j.exceptions import AuthError

from pyneo4j_ogm.core.client import Neo4jClient
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    UnsupportedDatabaseVersionError,
)
from tests.fixtures.db import Authentication, ConnectionString


class TestNeo4jClient:
    def test_str_representation(self):
        client = Neo4jClient()

        as_string = str(client)
        assert "(Neo4j)" in as_string

    @pytest.mark.asyncio
    async def test_successful_connect(self):
        client = Neo4jClient()
        await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    @pytest.mark.asyncio
    async def test_checks_connectivity_and_auth(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch(
            "neo4j.AsyncDriver.verify_connectivity", wraps=async_driver_mock.verify_connectivity
        ) as verify_connectivity_spy:
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

            verify_connectivity_spy.assert_called()

    @pytest.mark.asyncio
    async def test_validates_neo4j_version(self):
        server_info_mock = AsyncMock()
        server_info_mock.agent = "Neo4j/4.9.0"

        with patch("neo4j.AsyncDriver.get_server_info", return_value=server_info_mock):
            with pytest.raises(UnsupportedDatabaseVersionError):
                client = Neo4jClient()
                await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

    @pytest.mark.asyncio
    async def test_raises_on_invalid_uri(self):
        with pytest.raises(ValueError):
            client = Neo4jClient()
            await client.connect("bolt://invalid-connection:9999")

    @pytest.mark.asyncio
    async def test_raises_on_invalid_auth(self):
        with pytest.raises(AuthError):
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value)

    @pytest.mark.asyncio
    async def test_is_chainable(self):
        client = Neo4jClient()
        chainable = await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

        assert isinstance(chainable, Neo4jClient)
        assert chainable == client

    @pytest.mark.asyncio
    async def test_connected_state(self):
        client = Neo4jClient()

        is_connected = await client.connected()
        assert is_connected is False

        await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
        is_connected = await client.connected()
        assert is_connected is True

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
    async def test_close_raises_if_not_initialized(self):
        client = Neo4jClient()

        with pytest.raises(ClientNotInitializedError):
            await client.close()

    @pytest.mark.asyncio
    async def test_closes_driver_connection(self):
        async_driver_mock = AsyncMock(spec=neo4j.AsyncDriver)

        with patch("neo4j.AsyncDriver.close", wraps=async_driver_mock.close) as close_spy:
            client = Neo4jClient()
            await client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)

            await client.close()
            close_spy.assert_called()
            assert client._driver is None
