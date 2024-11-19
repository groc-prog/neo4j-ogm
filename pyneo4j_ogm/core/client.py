"""
Clients module containing abstract base class for all clients and client implementations
for both Neo4j and Memgraph.
"""

import importlib.util
import inspect
import os
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from functools import wraps
from logging import Logger
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    List,
    LiteralString,
    Optional,
    Self,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession, AsyncTransaction, Query

from pyneo4j_ogm.core.node import NodeModel
from pyneo4j_ogm.core.relationship import RelationshipModel
from pyneo4j_ogm.exceptions import (
    ClientNotInitializedError,
    ModelResolveError,
    NoTransactionInProgress,
    TransactionInProgress,
    UnsupportedDatabaseVersionError,
)
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.types.memgraph import MemgraphIndexType


def initialize_models_after(func):
    """
    Triggers model initialization for creating indexes/constraints and doing other setup work.

    Args:
        func (Callable): The function to be decorated, which can be either synchronous
            or asynchronous.

    Raises:
        ClientNotInitializedError: The client is not initialized yet.

    Returns:
        Callable: A wrapped function that includes additional functionality for both
            sync and async functions.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs) -> None:
        if getattr(self, "_driver", None) is not None:
            initialize = cast(Optional[Callable], getattr(self, "_initialize_models", None))

            if initialize is None:
                raise ValueError("Model initialization function not found")

            await initialize()

        return await func(self, *args, **kwargs)

    return wrapper


def ensure_initialized(func):
    """
    Ensures the driver of the client is initialized before interacting with the database.

    Args:
        func (Callable): The function to be decorated.

    Raises:
        ClientNotInitializedError: The client is not initialized yet.

    Returns:
        A wrapped function that includes additional functionality.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if getattr(self, "_driver", None) is None:
            raise ClientNotInitializedError()

        result = await func(self, *args, **kwargs)
        return result

    logger.debug("Ensuring client is initialized")
    wrapper.__annotations__ = func.__annotations__
    return wrapper


class Pyneo4jClient(ABC):
    """
    Base class for all client implementations.

    This client provides all basic functionality which all clients can use. Additionally, it also implements
    a interface for common methods all clients must implement in order to work with models. Methods for
    indexing/constraints are not added since Neo4j/Memgraph have differences in both how they do
    indexing/constraints and the existing types. To reduce complexity, which would be caused by generic methods,
    each client will implement it's own methods, which should follow a common naming scheme.
    """

    _driver: Optional[AsyncDriver]
    _session: Optional[AsyncSession]
    _transaction: Optional[AsyncTransaction]
    _models: Set[Union[Type[NodeModel], Type[RelationshipModel]]]
    _initialized_models: Set[Union[Type[NodeModel], Type[RelationshipModel]]]
    _skip_constraint_creation: bool
    _skip_index_creation: bool
    _using_batches: bool
    _logger: Logger

    def __init__(self) -> None:
        super().__init__()

        self._logger = logger.getChild(str(self))
        self._logger.debug("Initializing client")

        self._driver = None
        self._session = None
        self._transaction = None
        self._models = set()
        self._initialized_models = set()
        self._skip_constraint_creation = False
        self._skip_index_creation = False
        self._using_batches = False

    @abstractmethod
    async def drop_constraints(self) -> Self:
        """
        Drops all existing constraints.
        """
        pass

    @abstractmethod
    async def drop_indexes(self) -> Self:
        """
        Drops all existing indexes.
        """
        pass

    @abstractmethod
    async def _check_database_version(self) -> None:
        """
        Checks if the connected database is running a supported version.

        Raises:
            UnsupportedDatabaseVersionError: Connected to a database with a unsupported version.
        """
        pass

    @abstractmethod
    async def _initialize_models(self) -> None:
        """
        Initializes all registered models by setting the defined indexes/constraints. This
        method has to be implemented by each client because of differences in index/constraint
        creation. All registered models have to be added to the `_initialized_models` set to
        allow tracking of models which have not been initialized yet.
        """
        pass

    async def connected(self) -> bool:
        """
        Checks if the client is already connected or not. If the client has been connected, but
        no connection can be established, or the authentication details are invalid, `False` is
        returned.

        Returns:
            bool: `True` if the client is connected and ready, otherwise `False`.
        """
        try:
            self._logger.info("Checking client connection and authentication")
            if self._driver is None:
                self._logger.debug("Client not initialized yet")
                return False

            self._logger.debug("Verifying connectivity to database")
            await self._driver.verify_connectivity()
            return True
        except Exception as exc:
            self._logger.error(exc)
            return False

    @initialize_models_after
    async def connect(
        self,
        uri: str,
        *args,
        skip_constraints: bool = False,
        skip_indexes: bool = False,
        **kwargs,
    ) -> Self:
        """
        Connects to the specified Neo4j/Memgraph database. This method also accepts the same arguments
        as the Neo4j Python driver.

        Args:
            uri (str): The URI to connect to.
            skip_constraints (bool): Whether to skip creating any constraints defined by models. Defaults
                to `False`.
            skip_indexes (bool): Whether to skip creating any indexes defined by models. Defaults to
                `False`.

        Returns:
            Self: The client instance, which allows for chained calls.
        """
        self._skip_constraint_creation = skip_constraints
        self._skip_index_creation = skip_indexes

        self._logger.info("Connecting to database %s with %s", uri, self)
        self._driver = AsyncGraphDatabase.driver(uri=uri, *args, **kwargs)

        self._logger.debug("Checking connectivity and authentication")
        await self._driver.verify_connectivity()

        self._logger.debug("Checking for compatible database version")
        await self._check_database_version()

        self._logger.info("%s connected to database %s", uri, self)
        return self

    @ensure_initialized
    async def close(self) -> None:
        """
        Closes the connection to the database.
        """
        self._logger.info("Closing database connection for %s", self)
        await cast(AsyncDriver, self._driver).close()
        self._driver = None
        self._logger.info("Connection to database for %s closed", self)

    @ensure_initialized
    async def cypher(
        self,
        query: Union[str, LiteralString, Query],
        parameters: Optional[Dict[str, Any]] = None,
        auto_committing: bool = False,
        resolve_models: bool = False,
        raise_on_resolve_exc: bool = False,
    ) -> Tuple[List[List[Any]], List[str]]:
        """
        Runs the defined Cypher query with the given parameters. Returned nodes/relationships
        can be resolved to `registered models` by settings the `resolve_models` parameter to `True`.
        By default, the model parsing will not raise an exception if it fails. This can be changed
        with the `raise_on_resolve_exc` parameter.

        **Note:** When using `Memgraph as a database`, some queries which have info reporting do not allow
        the usage of multicommand transactions. To still be able to run the query, you can set the
        `auto_committing` parameter to `True`. In doing so, the query will be run using a new session
        rather than a current transaction. This also meant that those queries `will not be batched` with others
        when using `with_batching`.

        Args:
            query (Union[str, LiteralString, Query]): Neo4j Query class or query string. Same as queries
                for the Neo4j driver.
            parameters (Optional[Dict[str, Any]]): Optional parameters used by the query. Same as parameters
                for the Neo4j driver. Defaults to `None`.
            auto_committing (bool): Whether to use session or transaction for running the query. Can be used for
                Memgraph queries using info reporting. Defaults to `false`.
            resolve_models (bool): Whether to attempt to resolve the nodes/relationships returned by the query
                to their corresponding models. Models must be registered for this to work. Defaults to `False`.
            raise_on_resolve_exc (bool): Whether to silently fail or raise a `ModelResolveError` error if resolving
                a node/relationship fails. Defaults to `False`.

        Returns:
            Tuple[List[List[Any]], List[str]]: A tuple containing the query result and the names of the returned
                variables.
        """
        query_parameters: Dict[str, Any] = {}

        if parameters is not None and isinstance(parameters, dict):
            query_parameters = parameters

        if auto_committing:
            return await self._with_auto_committing_transaction(
                query, query_parameters, resolve_models, raise_on_resolve_exc
            )
        else:
            return await self._with_implicit_transaction(query, query_parameters, resolve_models, raise_on_resolve_exc)

    @initialize_models_after
    async def register_models(self, models: List[Union[Type[NodeModel], Type[RelationshipModel]]]) -> Self:
        """
        Registers the provided models with the client. Can be omitted if automatic index/constraint creation
        and resolving models in queries is not required.

        Args:
            models (List[Union[Type[NodeModel], Type[RelationshipModel]]]): The models to register. Invalid model
                instances will be skipped during the registration.

        Returns:
            Self: The client instance, which allows for chained calls.
        """
        self._logger.debug("Registering models with client %s", self)
        original_count = len(self._models)

        for model in models:
            if not issubclass(model, (NodeModel, RelationshipModel)):
                continue

            self._logger.debug("Registering model %s", model.__class__.__name__)
            self._models.add(model)

        current_count = len(self._models) - original_count
        self._logger.info("Registered %s models", current_count)

        return self

    @initialize_models_after
    async def register_models_directory(self, path: str) -> Self:
        """
        Recursively imports all discovered models from a given directory path and registers
        them with the client.

        Args:
            path (str): The path to the directory.

        Returns:
            Self: The client instance, which allows for chained calls.
        """
        self._logger.debug("Registering models in directory %s", path)
        original_count = len(self._models)

        for root, _, files in os.walk(path):
            self._logger.debug("Checking %s files for models", len(files))
            for file in files:
                if not file.endswith(".py"):
                    continue

                filepath = os.path.join(root, file)

                self._logger.debug("Found file %s, importing", filepath)
                module_name = os.path.splitext(os.path.basename(filepath))[0]
                spec = importlib.util.spec_from_file_location(module_name, filepath)

                if spec is None or spec.loader is None:
                    raise ImportError(f"Could not import file {filepath}")

                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                for member in inspect.getmembers(
                    module,
                    lambda x: inspect.isclass(x)
                    and issubclass(x, (NodeModel, RelationshipModel))
                    and x is not NodeModel
                    and x is not RelationshipModel,
                ):
                    self._models.add(member[1])

        current_count = len(self._models) - original_count
        self._logger.info("Registered %s models", current_count)

        return self

    @asynccontextmanager
    async def with_batching(self) -> AsyncGenerator[None, Any]:
        """
        Batches all queries called inside this context manager into a single transaction. Inside
        the context, both client queries and model methods can be called.
        """
        try:
            self._using_batches = True
            self._logger.info("Starting batch transaction")
            await self._begin_transaction()

            yield None

            self._logger.info("Batching transaction finished")
            await self._commit_transaction()
        except Exception as exc:
            self._logger.error(exc)
            await self._rollback_transaction()
            raise exc
        finally:
            self._using_batches = False

    @ensure_initialized
    async def drop_nodes(self) -> Self:
        """
        Deletes all nodes and relationships.
        """
        self._logger.warning("Dropping all nodes and relationships")
        await self.cypher("MATCH (n) DETACH DELETE n")

        self._logger.info("All nodes and relationships deleted")
        return self

    @ensure_initialized
    async def _begin_transaction(self) -> None:
        """
        Checks for existing sessions/transactions and begins new ones if none exist.

        Raises:
            TransactionInProgress: A session/transaction is already in progress.
        """
        if self._session is not None or self._transaction is not None:
            raise TransactionInProgress()

        self._logger.debug("Acquiring new session")
        self._session = cast(AsyncDriver, self._driver).session()
        self._logger.debug("Session %s acquired", self._session)

        self._logger.debug("Starting new transaction for session %s", self._session)
        self._transaction = await self._session.begin_transaction()
        self._logger.debug("Transaction %s for session %s acquired", self._transaction, self._session)

    @ensure_initialized
    async def _commit_transaction(self) -> None:
        """
        Commits the current transaction and closes it.

        Raises:
            NoTransactionInProgress: No active session/transaction to commit.
        """
        if self._session is None or self._transaction is None:
            raise NoTransactionInProgress()

        self._logger.debug("Committing transaction %s and closing session %s", self._transaction, self._session)
        await self._transaction.commit()
        self._transaction = None
        self._logger.debug("Transaction committed")

        await self._session.close()
        self._session = None
        self._logger.debug("Session closed")

    @ensure_initialized
    async def _rollback_transaction(self) -> None:
        """
        Rolls the current transaction back and closes it.

        Raises:
            NoTransactionInProgress: No active session/transaction to roll back.
        """
        if self._session is None or self._transaction is None:
            raise NoTransactionInProgress()

        self._logger.debug("Rolling back transaction %s and closing session %s", self._transaction, self._session)
        await self._transaction.rollback()
        self._transaction = None
        self._logger.debug("Transaction rolled back")

        await self._session.close()
        self._session = None
        self._logger.debug("Session closed")

    async def _with_implicit_transaction(
        self,
        query: Union[str, LiteralString, Query],
        parameters: Dict[str, Any],
        resolve_models: bool,
        raise_on_resolve_exc: bool,
    ) -> Tuple[List[List[Any]], List[str]]:
        """
        Runs a query with manually handled transactions, allowing for batching and finer control over
        committing/rollbacks.

        Args:
            query (Union[str, LiteralString, Query]): Neo4j Query class or query string. Same as queries
                for the Neo4j driver.
            parameters (Optional[Dict[str, Any]]): Optional parameters used by the query. Same as parameters
                for the Neo4j driver. Defaults to `None`.
            resolve_models (bool): Whether to attempt to resolve the nodes/relationships returned by the query
                to their corresponding models. Models must be registered for this to work. Defaults to `False`.
            raise_on_resolve_exc (bool): Whether to silently fail or raise a `ModelResolveError` error if resolving
                a node/relationship fails. Defaults to `False`.

        Raises:
            ModelResolveError: `raise_on_resolve_exc` is set to `True` and resolving a result fails.

        Returns:
            Tuple[List[List[Any]], List[str]]: A tuple containing the query result and the names of the returned
                variables.
        """
        if not self._using_batches:
            # If we are currently using batching, we should already be inside a active session/transaction
            await self._begin_transaction()

        try:
            self._logger.info("%s with parameters %s", query, parameters)
            query_result = await cast(AsyncTransaction, self._transaction).run(cast(LiteralString, query), parameters)

            self._logger.debug("Parsing query results")
            results = [list(result.values()) async for result in query_result]
            keys = list(query_result.keys())

            if resolve_models:
                try:
                    # TODO: Try to resolve models and raise an exception depending on the parameters provided
                    pass
                except Exception as exc:
                    self._logger.warning("Resolving models failed with %s", exc)
                    if raise_on_resolve_exc:
                        raise ModelResolveError() from exc

            if not self._using_batches:
                # Again, don't commit anything to the database when batching is enabled
                await self._commit_transaction()

            return results, keys
        except Exception as exc:
            self._logger.error("Query exception: %s", exc)

            if not self._using_batches:
                # Same as in the beginning, we don't want to roll back anything if we use batching
                await self._rollback_transaction()

            raise exc

    async def _with_auto_committing_transaction(
        self,
        query: Union[str, LiteralString, Query],
        parameters: Dict[str, Any],
        resolve_models: bool,
        raise_on_resolve_exc: bool,
    ) -> Tuple[List[List[Any]], List[str]]:
        """
        Runs a auto-committing query using a session rather than a transaction. This has to be used
        with some Memgraph queries due to some restrictions, though this mainly concerns queries
        with info reporting (`SHOW INDEX INFO` for example).

        Args:
            query (Union[str, LiteralString, Query]): Neo4j Query class or query string. Same as queries
                for the Neo4j driver.
            parameters (Optional[Dict[str, Any]]): Optional parameters used by the query. Same as parameters
                for the Neo4j driver. Defaults to `None`.
            resolve_models (bool): Whether to attempt to resolve the nodes/relationships returned by the query
                to their corresponding models. Models must be registered for this to work. Defaults to `False`.
            raise_on_resolve_exc (bool): Whether to silently fail or raise a `ModelResolveError` error if resolving
                a node/relationship fails. Defaults to `False`.

        Raises:
            ModelResolveError: `raise_on_resolve_exc` is set to `True` and resolving a result fails.

        Returns:
            Tuple[List[List[Any]], List[str]]: A tuple containing the query result and the names of the returned
                variables.
        """
        try:
            self._logger.debug("Acquiring new session")
            session = cast(AsyncDriver, self._driver).session()
            self._logger.debug("Session %s acquired", session)

            self._logger.info("%s with parameters %s", query, parameters)
            query_result = await session.run(cast(LiteralString, query), parameters)

            self._logger.debug("Parsing query results")
            results = [list(result.values()) async for result in query_result]
            keys = list(query_result.keys())

            if resolve_models:
                try:
                    # TODO: Try to resolve models and raise an exception depending on the parameters provided
                    pass
                except Exception as exc:
                    self._logger.warning("Resolving models failed with %s", exc)
                    if raise_on_resolve_exc:
                        raise ModelResolveError() from exc

            self._logger.debug("Closing session %s", session)
            await session.close()
            self._logger.debug("Session closed")

            return results, keys
        except Exception as exc:
            self._logger.error("Query exception: %s", exc)
            raise exc


class Neo4jClient(Pyneo4jClient):
    """
    Neo4j client used for interacting with a Neo4j database. Provides basic functionality for querying, indexing,
    constraints and other utilities.
    """

    def __str__(self) -> str:
        return f"(Neo4j){hex(id(self))}"

    @ensure_initialized
    async def drop_constraints(self) -> Self:
        self._logger.debug("Discovering constraints")
        constraints, _ = await self.cypher("SHOW CONSTRAINTS")

        self._logger.warning("Dropping all constraints")
        for constraint in constraints:
            self._logger.debug("Dropping constraint %s", constraint[1])
            await self.cypher(f"DROP CONSTRAINT {constraint[1]}")

        self._logger.debug("Dropped %s constraints", len(constraints))
        return self

    @ensure_initialized
    async def drop_indexes(self) -> Self:
        self._logger.debug("Discovering indexes")
        indexes, _ = await self.cypher("SHOW INDEXES")

        self._logger.warning("Dropping all indexes")
        for index in indexes:
            self._logger.debug("Dropping index %s", index[1])
            await self.cypher(f"DROP INDEX {index[1]}")

        self._logger.debug("Dropped %s indexes", len(indexes))
        return self

    @ensure_initialized
    async def _check_database_version(self) -> None:
        self._logger.debug("Checking if Neo4j version is supported")
        server_info = await cast(AsyncDriver, self._driver).get_server_info()

        version = server_info.agent.split("/")[1]

        if int(version.split(".")[0]) < 5:
            raise UnsupportedDatabaseVersionError()

    @ensure_initialized
    async def _initialize_models(self) -> None:
        pass


class MemgraphClient(Pyneo4jClient):
    """
    Memgraph client used for interacting with a Memgraph database. Provides basic functionality for querying, indexing,
    constraints and other utilities.
    """

    def __str__(self) -> str:
        return f"(Memgraph){hex(id(self))}"

    async def drop_constraints(self) -> Self:
        return await super().drop_constraints()

    async def drop_indexes(self) -> Self:
        self._logger.debug("Discovering indexes")
        indexes, _ = await self.cypher("SHOW INDEX INFO", auto_committing=True)

        for index in indexes:
            match index[0]:
                case MemgraphIndexType.EDGE_TYPE.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.EDGE_TYPE_AND_PROPERTY.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.LABEL.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.LABEL_AND_PROPERTY.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.POINT.value:
                    await self.cypher(f"DROP POINT INDEX ON :{index[1]}({index[2]})", auto_committing=True)

        return self

    @ensure_initialized
    async def _check_database_version(self) -> None:
        # I'm not sure if we actually need/can to check anything here since the server info
        # only states 'Neo4j/v5.11.0 compatible graph database server - Memgraph'
        pass

    @ensure_initialized
    async def _initialize_models(self) -> None:
        pass
