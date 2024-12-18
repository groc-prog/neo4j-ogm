import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, Optional, Set, cast

from pyneo4j_ogm.logger import logger

if TYPE_CHECKING:
    from pyneo4j_ogm.clients.base import Pyneo4jClient  # pragma: no cover
else:
    Pyneo4jClient = object


class Registry:
    """
    Thread-safe singleton client registry. All used clients have to be registered with this registry to
    be injected/used in models. When using multi-threading, each thread will have it's own registry and
    will need to register it's own clients.
    """

    _thread_ctx = threading.local()

    def __new__(cls):
        if not hasattr(cls._thread_ctx, "instance"):
            instance = super(Registry, cls).__new__(cls)

            setattr(instance._thread_ctx, "clients", set())
            setattr(instance._thread_ctx, "active_client", None)
            setattr(cls._thread_ctx, "instance", instance)

        return cast(Registry, getattr(cls._thread_ctx, "instance"))

    @property
    def active_client(self) -> Optional[Pyneo4jClient]:
        """
        Gets the currently active client. Each thread has it's own active client and registry.

        Returns:
            Optional[Pyneo4jClient]: Either the currently active client or `None` if no clients are available.
        """
        return getattr(self._thread_ctx, "active_client", None)

    def register(self, client: Pyneo4jClient) -> None:
        """
        Registers multiple clients with the registry. Only registered clients will be injected into a model
        instance/method to run queries.

        Args:
            client (Pyneo4jClient): The client to register.

        Raises:
            ValueError: If a invalid client is provided.
        """
        from pyneo4j_ogm.clients.base import Pyneo4jClient

        logger.debug("Registering client %s with registry", client)
        registered_clients = cast(Set[Pyneo4jClient], getattr(self._thread_ctx, "clients"))

        if registered_clients is None or not isinstance(client, Pyneo4jClient):
            raise ValueError("Client must be a instance of `Pyneo4jClient`")

        registered_clients.add(client)

        if getattr(self._thread_ctx, "active_client", None) is None and len(registered_clients) > 0:
            setattr(self._thread_ctx, "active_client", client)

    def deregister(self, client: Pyneo4jClient) -> None:
        """
        De-registers a previously registered client. If the provided client is the active client,
        a new client will be set as the active one. If no more clients are registered, the active
        client will be set to `None`.

        Args:
            client (Pyneo4jClient): The client to de-register.
        """
        registered_clients = cast(Set[Pyneo4jClient], getattr(self._thread_ctx, "clients", set()))
        if client not in registered_clients:
            logger.debug("Client is not registered, skipping")
            return

        logger.debug("De-registering client %s", client)
        registered_clients.remove(client)

        # NOTE: Maybe we want to raise an exception here instead so we don't get some **magic** behavior
        # if someone de-registers the currently active client
        if self.active_client == client:
            logger.debug("Active client de-registered, switching active client")

            if len(registered_clients) > 0:
                logger.debug("Other available client found")
                setattr(self._thread_ctx, "active_client", next(iter(registered_clients)))
            else:
                logger.debug("No other registered clients found")
                setattr(self._thread_ctx, "active_client", None)

    def set_active_client(self, client: Optional[Pyneo4jClient]) -> None:
        """
        Updates the active client.

        Args:
            client (Optional[Pyneo4jClient]): The client to set as active or `None`.

        Raises:
            ValueError: If `client` is not an instance of `Pyneo4jClient`.
        """
        from pyneo4j_ogm.clients.base import Pyneo4jClient

        if client is not None and (
            not isinstance(client, Pyneo4jClient)
            or client not in cast(Set[Pyneo4jClient], getattr(self._thread_ctx, "clients", set()))
        ):
            raise ValueError("Client must be a instance of `Pyneo4jClient`")

        setattr(self._thread_ctx, "active_client", client)


@contextmanager
def with_client(client: Pyneo4jClient) -> Generator[Pyneo4jClient, Any, None]:
    """
    Temporarily sets the specified client as the active client within a context.

    This context manager sets the provided `client` as the active client for the
    current thread, allowing operations to use this client within the scope of the
    `with` block. When the context exits, the active client is reverted to its
    previous value.

    The provided client must be registered prior to using it, otherwise a `ValueError`
    is raised.

    Args:
        client (Pyneo4jClient): The client instance to set as active within the context.

    Yields:
        Pyneo4jClient: The active client set for the context.

    Example:
        ```python
        client_one = Pyneo4jClient() # Currently active
        client_two = Pyneo4jClient()

        with with_client(client_two) as client:
            # Within this block, the active client is `client_two`.
            pass

        # Outside the block, the active client is reverted back to `client_one`.
        ```
    """
    registry = Registry()

    original_client = registry.active_client
    registry.set_active_client(client)

    try:
        logger.info("Entering context with scoped client %s", client)
        yield cast(Pyneo4jClient, registry.active_client)
    finally:
        registry.set_active_client(original_client)
