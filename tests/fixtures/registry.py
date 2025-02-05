import pytest

from pyneo4j_ogm.registry import Registry


@pytest.fixture(autouse=True)
def reset_registry_state():
    setattr(Registry._thread_ctx, "clients", set())
    setattr(Registry._thread_ctx, "active_client", None)

    yield

    setattr(Registry._thread_ctx, "clients", set())
    setattr(Registry._thread_ctx, "active_client", None)
