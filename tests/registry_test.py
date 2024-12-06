# pylint: disable=missing-class-docstring, redefined-outer-name
import threading
from typing import List
from unittest.mock import MagicMock

import pytest

from pyneo4j_ogm.clients.base import Pyneo4jClient
from pyneo4j_ogm.exceptions import InvalidClientError
from pyneo4j_ogm.registry import Registry, with_client


@pytest.fixture(autouse=True)
def reset_registry_state():
    setattr(Registry._thread_ctx, "clients", set())
    setattr(Registry._thread_ctx, "active_client", None)

    yield

    setattr(Registry._thread_ctx, "clients", set())
    setattr(Registry._thread_ctx, "active_client", None)


@pytest.fixture
def registry():
    return Registry()


class TestRegistry:
    def test_registry_singleton(self, registry):
        registry_one = registry
        registry_two = registry

        assert registry_one == registry_two

    def test_register_single_client(self, registry):
        client = MagicMock(spec=Pyneo4jClient)
        registry.register(client)

        assert client in registry._thread_ctx.clients

    def test_register_multiple_clients(self, registry):
        client1 = MagicMock(spec=Pyneo4jClient)
        client2 = MagicMock(spec=Pyneo4jClient)
        registry.register(client1)
        registry.register(client2)

        assert client1 in registry._thread_ctx.clients
        assert client2 in registry._thread_ctx.clients

    def test_register_non_client(self, registry):
        class NotAClient:
            pass

        client1 = MagicMock(spec=Pyneo4jClient)
        client2 = MagicMock(spec=Pyneo4jClient)
        registry.register(client1)
        registry.register(client2)

        with pytest.raises(ValueError):
            registry.register(NotAClient())

        assert client1 in registry._thread_ctx.clients
        assert client2 in registry._thread_ctx.clients
        assert len(registry._thread_ctx.clients) == 2

    def test_register_and_set_active_client(self, registry):
        client1 = MagicMock(spec=Pyneo4jClient)
        client2 = MagicMock(spec=Pyneo4jClient)
        registry.register(client1)
        registry.register(client2)

        assert registry.active_client == client1

    def test_deregister_client(self, registry):
        client = MagicMock(spec=Pyneo4jClient)
        registry.register(client)

        registry.deregister(client)

        assert client not in registry._thread_ctx.clients

    def test_deregister_active_client(self, registry):
        client1 = MagicMock(spec=Pyneo4jClient)
        client2 = MagicMock(spec=Pyneo4jClient)
        registry.register(client1)
        registry.register(client2)

        registry.deregister(client1)

        assert registry.active_client == client2

    def test_deregister_and_empty_registry(self, registry):
        client = MagicMock(spec=Pyneo4jClient)
        registry.register(client)

        registry.deregister(client)

        assert registry.active_client is None

    def test_deregister_unregistered_client(self, registry):
        client = MagicMock(spec=Pyneo4jClient)

        registry.deregister(client)

        assert registry.active_client is None

    def test_set_active_client(self, registry):
        client = MagicMock(spec=Pyneo4jClient)
        registry.register(client)

        registry.set_active_client(client)

        assert registry.active_client == client

    def test_invalid_client_set_active(self, registry):
        invalid_client = MagicMock(spec=Pyneo4jClient)

        with pytest.raises(InvalidClientError):
            registry.set_active_client(invalid_client)

    def test_set_active_client_to_none(self, registry):
        client = MagicMock(spec=Pyneo4jClient)
        registry.register(client)

        registry.set_active_client(None)

        assert registry.active_client is None


class TestRegistryMultiThreaded:
    def test_multithreading_registry_isolation(self):
        threads = []
        results = {}

        def register_clients_in_thread(clients: List[Pyneo4jClient], results: dict, thread_name: str):
            registry = Registry()

            for client in clients:
                registry.register(client)

            results[thread_name] = {
                "clients": registry._thread_ctx.clients,
                "active_client": registry.active_client,
            }

        thread_client_sets = [
            [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)],
            [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)],
            [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)],
        ]

        for idx, client_set in enumerate(thread_client_sets):
            thread_name = f"Thread-{idx}"
            thread = threading.Thread(target=register_clients_in_thread, args=(client_set, results, thread_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i, client_set_i in enumerate(thread_client_sets):
            thread_name_i = f"Thread-{i}"
            thread_clients_i = results[thread_name_i]["clients"]

            assert set(thread_clients_i) == set(client_set_i)

            for j, _ in enumerate(thread_client_sets):
                if i != j:
                    thread_name_j = f"Thread-{j}"
                    thread_clients_j = results[thread_name_j]["clients"]
                    assert set(thread_clients_i).isdisjoint(set(thread_clients_j))

        for idx, client_set in enumerate(thread_client_sets):
            thread_name = f"Thread-{idx}"
            active_client = results[thread_name]["active_client"]
            assert active_client in client_set

    def test_active_client_isolation_across_threads(self):
        threads = []
        results = {}

        def set_active_client_in_thread(clients: List[Pyneo4jClient], results, thread_name):
            registry = Registry()

            for client in clients:
                registry.register(client)

            registry.set_active_client(clients[1])
            results[thread_name] = registry.active_client

        for i in range(3):
            clients = [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)]
            thread_name = f"Thread-{i}"
            thread = threading.Thread(target=set_active_client_in_thread, args=(clients, results, thread_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(3):
            assert isinstance(results[f"Thread-{i}"], Pyneo4jClient)

    def test_concurrent_registration_deregistration(self):
        threads = []
        clients_per_thread = 3
        results = {}

        def register_and_deregister(clients: List[Pyneo4jClient], results, thread_name):
            registry = Registry()

            for client in clients:
                registry.register(client)

            registry.deregister(clients[0])
            results[thread_name] = {
                "remaining_clients": registry._thread_ctx.clients,
                "active_client": registry.active_client,
            }

        for i in range(3):
            clients = [MagicMock(spec=Pyneo4jClient) for _ in range(clients_per_thread)]
            thread_name = f"Thread-{i}"
            thread = threading.Thread(target=register_and_deregister, args=(clients, results, thread_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(3):
            thread_name = f"Thread-{i}"
            assert len(results[thread_name]["remaining_clients"]) == clients_per_thread - 1

    def test_with_client_context_manager_isolation(self):
        threads = []
        results = {}

        def use_with_client_context(clients: List[Pyneo4jClient], results, thread_name):
            registry = Registry()

            for client in clients:
                registry.register(client)

            original_client = registry.active_client

            with with_client(clients[1]):
                results[thread_name] = {
                    "original_client_before": original_client,
                    "within_context": registry.active_client,
                    "original_client_after": None,
                }

            results[thread_name]["original_client_after"] = registry.active_client

        for i in range(3):
            clients = [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)]
            thread_name = f"Thread-{i}"
            thread = threading.Thread(target=use_with_client_context, args=(clients, results, thread_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(3):
            thread_name = f"Thread-{i}"
            assert results[thread_name]["original_client_after"] == results[thread_name]["original_client_before"]
            assert results[thread_name]["within_context"] != results[thread_name]["original_client_before"]

    def test_thread_specific_deregister_active_client(self):
        threads = []
        results = {}

        def deregister_active_client(clients: List[Pyneo4jClient], results, thread_name):
            registry = Registry()

            for client in clients:
                registry.register(client)

            registry.set_active_client(clients[0])
            registry.deregister(clients[0])

            results[thread_name] = {
                "remaining_clients": registry._thread_ctx.clients,
                "new_active_client": registry.active_client,
            }

        for i in range(3):
            clients = [MagicMock(spec=Pyneo4jClient), MagicMock(spec=Pyneo4jClient)]
            thread_name = f"Thread-{i}"
            thread = threading.Thread(target=deregister_active_client, args=(clients, results, thread_name))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for i in range(3):
            thread_name = f"Thread-{i}"
            remaining_clients = results[thread_name]["remaining_clients"]
            assert len(remaining_clients) == 1
            assert results[thread_name]["new_active_client"] in remaining_clients


class TestWithClientFunction:
    def test_with_client_sets_active_client(self, registry):
        client_one = MagicMock(spec=Pyneo4jClient)
        client_two = MagicMock(spec=Pyneo4jClient)
        registry.register(client_one)
        registry.register(client_two)

        with with_client(client_two) as client:
            assert registry.active_client == client_two
            assert client == client_two

        assert registry.active_client == client_one

    def test_with_client_invalid_client(self):
        invalid_client = MagicMock(spec=Pyneo4jClient)

        with pytest.raises(InvalidClientError):
            with with_client(invalid_client):
                pass
