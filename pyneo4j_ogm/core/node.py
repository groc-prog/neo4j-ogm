"""
Holds the base node class `NodeModel` which is used to define database models for nodes.
It provides base functionality like de-/inflation, validation and methods for interacting with
the database for CRUD operations on nodes.
"""

import json
from copy import deepcopy
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    ParamSpec,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from neo4j.graph import Node
from pydantic import PrivateAttr

from pyneo4j_ogm.core.base import ModelBase, hooks
from pyneo4j_ogm.exceptions import (
    InstanceDestroyed,
    InstanceNotHydrated,
    InvalidFilters,
    NoResultFound,
    UnexpectedEmptyResult,
    UnregisteredModel,
)
from pyneo4j_ogm.fields.settings import NodeModelSettings, RelationshipModelSettings
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.pydantic_utils import (
    IS_PYDANTIC_V2,
    get_field_type,
    get_model_dump,
    get_model_dump_json,
    get_model_fields,
    parse_model,
)
from pyneo4j_ogm.queries.types import (
    MultiHopFilters,
    NodeFilters,
    Projection,
    QueryOptions,
)

if TYPE_CHECKING:
    from pyneo4j_ogm.fields.relationship_property import RelationshipProperty
    from pyneo4j_ogm.core.client import BatchManager
else:
    RelationshipProperty = object
    BatchManager = object

P = ParamSpec("P")
T = TypeVar("T", bound="NodeModel")


def ensure_alive(func):
    """
    Decorator to ensure that the decorated method is only called on a alive instance.

    Args:
        func (Callable): The method to decorate.

    Raises:
        InstanceDestroyed: If the instance is destroyed.
        InstanceNotHydrated: If the instance is not hydrated.

    Returns:
        Callable: The decorated method.
    """

    @wraps(func)
    def wrapper(self, *args: Any, **kwargs: Any):
        logger.debug("Checking if instance is alive and hydrated")
        if getattr(self, "_destroyed", False):
            raise InstanceDestroyed()

        if getattr(self, "_element_id", None) is None or getattr(self, "_id", None) is None:
            raise InstanceNotHydrated()

        return func(self, *args, **kwargs)

    return wrapper


class NodeModel(ModelBase[NodeModelSettings]):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.

    Provides methods for interacting with the database for CRUD operations on nodes. Settings can
    be defined by providing an inner class `Settings`. Settings can control the behavior of the
    model and are defined in `pyneo4j_ogm.fields.settings.NodeModelSettings`.
    """

    _settings: NodeModelSettings = PrivateAttr()
    _relationship_properties: Set[str] = PrivateAttr()
    Settings: ClassVar[Type[NodeModelSettings]]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        # Build relationship properties
        logger.debug("Building relationship properties for model %s", self.__class__.__name__)
        for relationship_property in self._relationship_properties:
            if IS_PYDANTIC_V2:
                # Pydantic V2 does not initialize separate instances for relationship properties
                # anymore, thus we have to do this manually here
                # This might be a dirty hack, but it works ¯\_(ツ)_/¯
                model_relationship_property = getattr(self, relationship_property)
                setattr(self, relationship_property, model_relationship_property)
                cast(RelationshipProperty, model_relationship_property)._build_property(self, relationship_property)
            else:
                model_relationship_property = getattr(self, relationship_property)

                if hasattr(model_relationship_property, "_build_property"):
                    cast(RelationshipProperty, model_relationship_property)._build_property(self, relationship_property)

        self._db_properties = get_model_dump(self, exclude={*self._relationship_properties, "element_id", "id"})

    def __init_subclass__(cls) -> None:
        setattr(cls, "_relationship_properties", set())

        inherited_settings = True
        if not isinstance(getattr(cls, "_settings", None), NodeModelSettings):
            inherited_settings = False
            setattr(cls, "_settings", NodeModelSettings())

        super().__init_subclass__()

        labels: Set[str] = getattr(cls._settings, "labels", set())
        if hasattr(cls, "Settings"):
            validated_settings = parse_model(NodeModelSettings, cls.Settings.__dict__)

            if len(validated_settings.labels) != 0 and not inherited_settings:
                labels = labels.union(validated_settings.labels)
            else:
                labels = labels.union({cls.__name__})
        else:
            labels = labels.union({cls.__name__})

        settings = deepcopy(cls._settings)
        settings.labels = labels
        cls._settings = settings

        if not IS_PYDANTIC_V2:
            cls._register_relationship_properties()

    if IS_PYDANTIC_V2:
        # Pydantic does not initialize either `__fields__` or `model_fields` in the __init_subclass__
        # method anymore in V2, thus we have to call this logic here as well
        @classmethod
        def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
            super().__pydantic_init_subclass__(**kwargs)

            cls._register_relationship_properties()

    @hooks
    async def create(self: T, batch_manager: Optional[BatchManager] = None) -> T:
        """
        Creates a new node from the current instance. After the method is finished, a newly created
        instance is seen as `hydrated` and all methods can be called on it.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.

        Returns:
            T: The current model instance.
        """
        logger.info("Creating new node from model %s", self.__class__.__name__)
        deflated_properties = self._deflate()

        set_query = (
            f"SET {', '.join(f'n.{property_name} = ${property_name}' for property_name in deflated_properties.keys())}"
            if len(deflated_properties.keys()) != 0
            else ""
        )

        results, _ = await self._client.cypher(
            query=f"""
                CREATE {self._query_builder.node_match(list(self._settings.labels))}
                {set_query}
                RETURN n
            """,
            parameters=deflated_properties,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        # Since the instance is now hydrated, we can set the element id and id and reset the modified properties
        # to the current instance values
        logger.debug("Hydrating instance values")
        setattr(self, "_element_id", getattr(cast(T, results[0][0]), "_element_id"))
        setattr(self, "_id", getattr(cast(T, results[0][0]), "_id"))

        logger.debug("Resetting modified properties")
        self._db_properties = get_model_dump(self, exclude={*self._relationship_properties, "element_id", "id"})
        logger.debug("Created new node %s", self)

        return self

    @hooks
    @ensure_alive
    async def update(self, batch_manager: Optional[BatchManager] = None) -> None:
        """
        Updates the corresponding node in the graph with the current instance values.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        deflated = self._deflate()

        logger.info(
            "Updating node %s with current properties %s",
            self,
            deflated,
        )
        set_query = ", ".join(
            [
                f"n.{property_name} = ${property_name}"
                for property_name in deflated
                if property_name in self.modified_properties
            ]
        )

        # We return the updated node to check if the query was successful
        # since Neo4j does not raise any exceptions if the node does not exist
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.node_match(list(self._settings.labels))}
                WHERE elementId(n) = $element_id
                {f"SET {set_query}" if set_query != "" else ""}
                RETURN n
            """,
            parameters={"element_id": self._element_id, **deflated},
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Resetting modified properties")
        self._db_properties = get_model_dump(self, exclude={*self._relationship_properties, "element_id", "id"})
        logger.debug("Updated node %s", self)

    @hooks
    @ensure_alive
    async def delete(self, batch_manager: Optional[BatchManager] = None) -> None:
        """
        Deletes the corresponding node in the graph and marks this instance as destroyed. If
        another method is called on this instance, an `InstanceDestroyed` will be raised.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        logger.info("Deleting node %s", self)
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.node_match(list(self._settings.labels))}
                WHERE elementId(n) = $element_id
                DETACH DELETE n
                RETURN count(n)
            """,
            parameters={"element_id": self._element_id},
            batch_manager=batch_manager,
        )

        # If the returned value is empty, the node does not exist and the query failed
        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Marking instance as destroyed")
        setattr(self, "_destroyed", True)
        logger.debug("Deleted node %s", self)

    @hooks
    @ensure_alive
    async def refresh(self, batch_manager: Optional[BatchManager] = None) -> None:
        """
        Refreshes the current instance with the corresponding values from the graph.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        logger.info("Refreshing node %s with values from database", self)
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.node_match(list(self._settings.labels))}
                WHERE elementId(n) = $element_id
                RETURN n
            """,
            parameters={"element_id": self._element_id},
            batch_manager=batch_manager,
        )

        # If the returned value is empty, we can not refresh the instance
        # since the node does not exist anymore
        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Updating current instance")
        self.__dict__.update(results[0][0].__dict__)
        logger.debug("Refreshed node %s", self)

    @hooks
    @ensure_alive
    async def find_connected_nodes(
        self,
        filters: MultiHopFilters,
        projections: Optional[Projection] = None,
        options: Optional[QueryOptions] = None,
        auto_fetch_nodes: Optional[bool] = None,
        auto_fetch_models: Optional[List[Union[str, Type["NodeModel"]]]] = None,
        batch_manager: Optional[BatchManager] = None,
    ) -> List[Union["NodeModel", Dict[str, Any]]]:
        """
        Gets all connected nodes which match the provided `filters` parameter over multiple hops.

        Args:
            filters (MultiHopFilters): The filters to apply to the query.
            projections (Projection, optional): The properties to project from the node. The keys define
                the new keys in the projection and the value defines the model property to be projected. A invalid
                or empty projection will result in the whole model instance being returned. Defaults to `None`.
            options (QueryOptions, optional): The options to apply to the query. Defaults to `None`.
            auto_fetch_nodes (bool, optional): Whether to automatically fetch connected nodes. Takes priority over the
                identical option defined in `Settings`. Defaults to `None`.
            auto_fetch_models (List[Union[str, Type["NodeModel"]]], optional): A list of models to auto-fetch.
                `auto_fetch_nodes` has to be set to `True` for this to have any effect. Defaults to `[]`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Raises:
            InvalidFilters: If auto-fetch is enabled and no node labels are provided.
            UnregisteredModel: If auto-fetch is enabled and a model with the provided labels is not registered.

        Returns:
            List["NodeModel" | Dict[str, Any]]: The nodes matched by the query or dictionaries of the projected
                properties.
        """
        target_node_model: Optional["NodeModel"] = None
        match_queries, return_queries = [], []
        do_auto_fetch: bool = False

        logger.info(
            "Getting connected nodes for node %s matching filters %s over multiple hops",
            self,
            filters,
        )

        # Build all queries and parameters needed for query
        self._query_builder.reset_query()
        self._query_builder.multi_hop_filters(filters=filters)

        if options is not None:
            self._query_builder.query_options(options=options)
        if projections is not None:
            self._query_builder.build_projections(projections=projections, ref="m")

        projection_query = (
            "RETURN m" if self._query_builder.query["projections"] == "" else self._query_builder.query["projections"]
        )

        if auto_fetch_nodes or (auto_fetch_nodes is not False and self._settings.auto_fetch_nodes):
            logger.debug("Auto-fetching nodes is enabled, checking if model with target labels is registered")
            if "$node" not in filters or "$labels" not in filters["$node"]:
                raise InvalidFilters()

            labels = set(filters["$node"]["$labels"])

            # Get target node model from the models registered with the client
            # If no model is found, someone forgot to register it
            for model in self._client.models:
                if not issubclass(model, NodeModel):
                    continue

                model_labels = set(getattr(model._settings, "labels", []))
                if not model_labels.difference(labels):
                    target_node_model = cast("NodeModel", model)
                    break

            if target_node_model is None:
                raise UnregisteredModel(f"with labels {labels}")

            logger.debug("Model with labels %s is registered, building auto-fetch query", labels)
            match_queries, return_queries = target_node_model._build_auto_fetch(  # pylint: disable=protected-access
                ref="m", nodes_to_fetch=auto_fetch_models
            )

            do_auto_fetch = all(
                [
                    len(match_queries) != 0,
                    len(return_queries) != 0,
                ]
            )

        logger.debug("Querying database with auto-fetch %s", "enabled" if do_auto_fetch else "disabled")
        results, meta = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.node_match(list(self._settings.labels))}{self._query_builder.query['match']}
                WHERE
                    elementId(n) = $element_id
                    {f"AND {self._query_builder.query['where']}" if self._query_builder.query['where'] != "" else ""}
                WITH DISTINCT m
                {self._query_builder.query['options']}
                {" ".join(f"OPTIONAL MATCH {match_query}" for match_query in match_queries) if do_auto_fetch else ""}
                {projection_query}{f', {", ".join(return_queries)}' if do_auto_fetch else ''}
            """,
            parameters={
                "element_id": self._element_id,
                **self._query_builder.parameters,
            },
            batch_manager=batch_manager,
        )

        instances: List[Union["NodeModel", Dict[str, Any]]] = []

        logger.debug("Building instances from results")
        if do_auto_fetch:
            # Add auto-fetched nodes to relationship properties
            # We need to keep track of which nodes have been added to which relationship property
            instance_map: Dict[str, Set[str]] = {}

            logger.debug("Adding auto-fetched nodes to relationship properties")
            for result_list in results:
                for index, result in enumerate(result_list):
                    target_instance_element_id = getattr(result_list[0], "_element_id", None)
                    instance_element_id = getattr(result, "_element_id", None)

                    if instance_element_id is None or target_instance_element_id is None:
                        continue

                    # If we are at the first result, we are dealing with a instance of the target node model
                    # and need to add it to the instances list
                    if index == 0 and target_node_model is not None and instance_element_id not in instance_map:
                        instances.append(
                            target_node_model._inflate(graph_entity=result) if isinstance(result, Node) else result
                        )
                        instance_map[instance_element_id] = set()

                    # If we are not at the first result, we are dealing with a instance of a auto-fetched
                    # node and need to add it to the relationship property of the target node model
                    elif index != 0 and instance_element_id not in instance_map[target_instance_element_id]:
                        target_instance = [
                            instance
                            for instance in instances
                            if getattr(result_list[0], "_element_id") == getattr(instance, "_element_id")
                        ][0]

                        # The meta list contains the names of the relationship properties
                        relationship_property = getattr(target_instance, meta[index])
                        nodes = cast(List[str], getattr(relationship_property, "_nodes"))
                        nodes.append(result)

                        instance_map[target_instance_element_id].add(instance_element_id)
        else:
            for result_list in results:
                for result in result_list:
                    if result is None:
                        continue

                    if isinstance(result, list):
                        instances.extend(result)
                    else:
                        instances.append(result)

        return instances

    @classmethod
    @hooks
    async def find_one(
        cls: Type[T],
        filters: NodeFilters,
        projections: Optional[Projection] = None,
        auto_fetch_nodes: Optional[bool] = None,
        auto_fetch_models: Optional[List[Union[str, Type["NodeModel"]]]] = None,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManager] = None,
    ) -> Optional[Union[T, Dict[str, Any]]]:
        """
        Finds the first node that matches `filters` and returns it. If no matching node is found,
        `None` is returned instead.

        Args:
            filters (NodeFilters): The filters to apply to the query.
            projections (Projection, optional): The properties to project from the node. The keys define
                the new keys in the projection and the value defines the model property to be projected. A invalid
                or empty projection will result in the whole model instance being returned. Defaults to `None`.
            auto_fetch_nodes (bool, optional): Whether to automatically fetch connected nodes. Takes priority over the
                identical option defined in `Settings`. Can not be used with projections. Defaults to `None`.
            auto_fetch_models (List[Union[str, Type["NodeModel"]]], optional): A list of models to auto-fetch.
                `auto_fetch_nodes` has to be set to `True` for this to have any effect. Defaults to `[]`.
            raise_on_empty (bool, optional): Whether to raise an `NoResultFound` if no match is found. Defaults to
                `False`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Raises:
            InvalidFilters: If no filters or invalid filters are provided.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            T | Dict[str, Any] | None: A instance of the model or None if no match is found or a dictionary of the
                projected properties.
        """
        logger.info(
            "Getting first encountered node of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.node_filters(filters=filters)

        if projections is not None:
            cls._query_builder.build_projections(projections=projections)

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        do_auto_fetch = all(
            [
                projections is None,
                auto_fetch_nodes or (auto_fetch_nodes is not False and cls._settings.auto_fetch_nodes),
                cls._query_builder.query["projections"] == "",
            ]
        )

        if do_auto_fetch:
            # If auto-fetch is enabled, we need to build the auto-fetch queries in addition to the normal query
            logger.debug("Querying database with auto-fetch enabled")
            projection_query = (
                "RETURN n" if cls._query_builder.query["projections"] == "" else cls._query_builder.query["projections"]
            )
            match_queries, return_queries = cls._build_auto_fetch(nodes_to_fetch=auto_fetch_models)

            results, meta = await cls._client.cypher(
                query=f"""
                    MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                    WHERE {cls._query_builder.query['where']}
                    WITH DISTINCT n
                    LIMIT 1
                    {" ".join(f"OPTIONAL MATCH {match_query}" for match_query in match_queries)}
                    {projection_query}, {', '.join(return_queries)}
                """,
                parameters=cls._query_builder.parameters,
                batch_manager=batch_manager,
            )
        else:
            logger.debug("Querying database without auto-fetch")
            projection_query = (
                "RETURN DISTINCT n"
                if cls._query_builder.query["projections"] == ""
                else cls._query_builder.query["projections"]
            )

            results, meta = await cls._client.cypher(
                query=f"""
                    MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                    WHERE {cls._query_builder.query['where']}
                    {projection_query}
                    LIMIT 1
                """,
                parameters=cls._query_builder.parameters,
                batch_manager=batch_manager,
            )

        logger.debug("Checking if query returned a result")
        if (
            len(results) == 0
            or len(results[0]) == 0
            or results[0][0] is None
            or (isinstance(results[0][0], dict) and len(results[0][0]) == 0)
        ):
            # If no results are found, we return None or raise an exception
            if raise_on_empty:
                raise NoResultFound(filters)
            return None

        # Normalize results to a single instance
        if isinstance(results[0][0], Node):
            instance = cls._inflate(graph_entity=results[0][0])
        elif isinstance(results[0][0], list):
            instance = results[0][0][0]
        else:
            instance = results[0][0]

        if not do_auto_fetch:
            return instance

        # Add auto-fetched nodes to relationship properties
        added_nodes: Set[str] = set()

        logger.debug("Adding auto-fetched nodes to relationship properties")
        for result_list in results:
            for index, result in enumerate(result_list[1:]):
                if result is not None:
                    relationship_property = getattr(instance, meta[index + 1])
                    nodes = cast(List[str], getattr(relationship_property, "_nodes"))
                    element_id = getattr(result, "_element_id")

                    if element_id not in added_nodes:
                        nodes.append(result)
                        added_nodes.add(element_id)

        return instance

    @classmethod
    @hooks
    async def find_many(
        cls: Type[T],
        filters: Optional[NodeFilters] = None,
        projections: Optional[Projection] = None,
        options: Optional[QueryOptions] = None,
        auto_fetch_nodes: Optional[bool] = None,
        auto_fetch_models: Optional[List[Union[str, Type["NodeModel"]]]] = None,
        batch_manager: Optional[BatchManager] = None,
    ) -> List[Union[T, Dict[str, Any]]]:
        """
        Finds the all nodes that matches `filters` and returns them. If no matches are found, an
        empty list is returned instead.

        Args:
            filters (NodeFilters, optional): The filters to apply to the query. Defaults to `None`.
            projections (Projection, optional): The properties to project from the node. The keys define
                the new keys in the projection and the value defines the model property to be projected. A invalid
                or empty projection will result in the whole model instance being returned. Defaults to `None`.
            options (QueryOptions, optional): The options to apply to the query. Defaults to `None`.
            auto_fetch_nodes (bool, optional): Whether to automatically fetch connected nodes. Takes priority over the
                identical option defined in `Settings`. Defaults to `None`.
            auto_fetch_models (List[Union[str, Type["NodeModel"]]], optional): A list of models to auto-fetch.
                `auto_fetch_nodes` has to be set to `True` for this to have any effect. Defaults to `[]`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Returns:
            List[T | Dict[str, Any]]: A list of model instances or dictionaries of the projected properties.
        """
        logger.info("Getting nodes of model %s matching filters %s", cls.__name__, filters)
        cls._query_builder.reset_query()

        if filters is not None:
            cls._query_builder.node_filters(filters=filters)
        if options is not None:
            cls._query_builder.query_options(options=options)
        if projections is not None:
            cls._query_builder.build_projections(projections=projections)

        instances: List[Union[T, Dict[str, Any]]] = []
        projection_query = (
            "RETURN n" if cls._query_builder.query["projections"] == "" else cls._query_builder.query["projections"]
        )

        do_auto_fetch = all(
            [
                projections is None,
                auto_fetch_nodes or (auto_fetch_nodes is not False and cls._settings.auto_fetch_nodes),
                cls._query_builder.query["projections"] == "",
            ]
        )

        if do_auto_fetch:
            logger.debug("Querying database with auto-fetch")
            match_queries, return_queries = cls._build_auto_fetch(nodes_to_fetch=auto_fetch_models)

            results, meta = await cls._client.cypher(
                query=f"""
                    MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                    {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                    WITH DISTINCT n
                    {cls._query_builder.query['options']}
                    {" ".join(f"OPTIONAL MATCH {match_query}" for match_query in match_queries)}
                    {projection_query}, {', '.join(return_queries)}
                """,
                parameters=cls._query_builder.parameters,
                batch_manager=batch_manager,
            )

            # Add auto-fetched nodes to relationship properties and keep track of which nodes
            # have been added already
            instance_map: Dict[str, Set[str]] = {}

            logger.debug("Adding auto-fetched nodes to relationship properties")
            for result_list in results:
                for index, result in enumerate(result_list):
                    target_instance_element_id = getattr(result_list[0], "_element_id", None)
                    instance_element_id = getattr(result, "_element_id", None)

                    if instance_element_id is None or target_instance_element_id is None:
                        continue

                    # If we are at the first result, we are dealing with a instance of the target node model
                    # and need to add it to the instances list
                    if index == 0 and instance_element_id not in instance_map:
                        instances.append(cls._inflate(graph_entity=result) if isinstance(result, Node) else result)

                        instance_map[instance_element_id] = set()

                    # If we are not at the first result, we are dealing with a instance of a auto-fetched
                    # node and need to add it to the relationship property of the target node model
                    elif index != 0 and instance_element_id not in instance_map[target_instance_element_id]:
                        target_instance = [
                            instance
                            for instance in instances
                            if getattr(result_list[0], "_element_id") == getattr(instance, "_element_id")
                        ][0]
                        relationship_property = getattr(target_instance, meta[index])
                        nodes = cast(List[str], getattr(relationship_property, "_nodes"))
                        nodes.append(result)

                        instance_map[target_instance_element_id].add(instance_element_id)

            return instances
        else:
            logger.debug("Querying database without auto-fetch")
            results, _ = await cls._client.cypher(
                query=f"""
                    MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                    {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                    {"WITH DISTINCT n" if cls._query_builder.query['options'] != "" else ""}
                    {cls._query_builder.query['options']}
                    {projection_query}
                """,
                parameters=cls._query_builder.parameters,
                batch_manager=batch_manager,
            )

            for result_list in results:
                for result in result_list:
                    if isinstance(result, Node):
                        instances.append(cls._inflate(graph_entity=result))
                    elif isinstance(result, list):
                        instances.extend(result)
                    elif isinstance(result, cls):
                        instances.append(result)

            return instances

    @classmethod
    @hooks
    async def update_one(
        cls: Type[T],
        update: Dict[str, Any],
        filters: NodeFilters,
        new: bool = False,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManager] = None,
    ) -> Optional[T]:
        """
        Finds the first node that matches `filters` and updates it with the values defined by
        `update`. If no match is found, `None` is returned instead.

        Args:
            update (Dict[str, Any]): Values to update the node properties with.
            filters (NodeFilters): The filters to apply to the query.
            new (bool, optional): Whether to return the updated node. By default, the old node is
                returned. Defaults to `False`.
            raise_on_empty (bool, optional): Whether to raise an `NoResultFound` if no match is found. Defaults to
                `False`.

        Raises:
            InvalidFilters: If no filters or invalid filters are provided.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            T | None: By default, the old node instance is returned. If `new` is set to `True`, the result
                will be the `updated` instance.
        """
        new_instance: T

        logger.info(
            "Updating first encountered node of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        logger.debug(
            "Getting first encountered node of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.node_filters(filters=filters)

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                WHERE {cls._query_builder.query['where']}
                RETURN DISTINCT n
                LIMIT 1
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            if raise_on_empty:
                raise NoResultFound(filters)
            return None

        old_instance = results[0][0] if isinstance(results[0][0], cls) else cls._inflate(graph_entity=results[0][0])

        # Update existing instance with values and save
        logger.debug("Creating instance copy with new values %s", update)
        new_instance = cls(**cast(T, old_instance)._deflate())

        for key, value in update.items():
            if key in get_model_fields(cls):
                setattr(new_instance, key, value)
        setattr(new_instance, "_element_id", getattr(old_instance, "_element_id", None))
        setattr(new_instance, "_id", getattr(old_instance, "_id", None))

        deflated = new_instance._deflate()
        set_query = ", ".join(
            [
                f"n.{property_name} = ${property_name}"
                for property_name in deflated
                if property_name in new_instance.modified_properties
            ]
        )

        await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(new_instance._settings.labels))}
                WHERE elementId(n) = $element_id
                {f"SET {set_query}" if set_query != "" else ""}
            """,
            parameters={"element_id": new_instance._element_id, **deflated},
            batch_manager=batch_manager,
        )
        logger.debug("Successfully updated node %s", getattr(new_instance, "_element_id"))

        if new:
            return new_instance

        return old_instance

    @classmethod
    @hooks
    async def update_many(
        cls: Type[T],
        update: Dict[str, Any],
        filters: Optional[NodeFilters] = None,
        new: bool = False,
        batch_manager: Optional[BatchManager] = None,
    ) -> List[T]:
        """
        Finds all nodes that match `filters` and updates them with the values defined by `update`.

        Args:
            update (Dict[str, Any]): Values to update the node properties with.
            filters (NodeFilters, optional): The filters to apply to the query. Defaults to `None`.
            new (bool, optional): Whether to return the updated nodes. By default, the old nodes
                is returned. Defaults to `False`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Returns:
            List[T]: By default, the old node instances are returned. If `new` is set to `True`,
                the result will be the `updated/created instances`.
        """
        new_instance: T

        logger.info("Updating all nodes of model %s matching filters %s", cls.__name__, filters)
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.node_filters(filters=filters)

        logger.debug("Getting all nodes of model %s matching filters %s", cls.__name__, filters)
        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                RETURN DISTINCT n
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        old_instances: List[T] = []

        for result_list in results:
            for result in result_list:
                if result is None:
                    continue

                old_instances.append(cls._inflate(graph_entity=result) if isinstance(result, Node) else result)

        logger.debug("Checking if query returned a result")
        if len(old_instances) == 0:
            logger.debug("No results found")
            return []

        # Try and parse update values into random instance to check validation
        logger.debug("Creating instance copy with new values %s", update)
        new_instance = cls(**old_instances[0]._deflate())
        for key, value in update.items():
            if key in get_model_fields(cls):
                setattr(new_instance, key, value)

        deflated_properties = new_instance._deflate()

        # Update instances
        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                SET {", ".join([f"n.{property_name} = ${property_name}" for property_name in deflated_properties if property_name in update])}
                RETURN DISTINCT n
            """,
            parameters={**deflated_properties, **cls._query_builder.parameters},
            batch_manager=batch_manager,
        )

        logger.debug(
            "Successfully updated %s nodes %s",
            len(old_instances),
            [getattr(instance, "_element_id") for instance in old_instances],
        )
        logger.debug("Building instances from results")
        if new:
            instances: List[T] = []

            for result_list in results:
                for result in result_list:
                    if result is not None:
                        instances.append(result)
            return instances
        return old_instances

    @classmethod
    @hooks
    async def delete_one(
        cls: Type[T],
        filters: NodeFilters,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManager] = None
    ) -> int:
        """
        Finds the first node that matches `filters` and deletes it. If no match is found, a
        `UnexpectedEmptyResult` is raised.

        Args:
            filters (NodeFilters): The filters to apply to the query.
            raise_on_empty (bool, optional): Whether to raise an `NoResultFound` if no match is found. Defaults to
                `False`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
            InvalidFilters: If no filters or invalid filters are provided.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            int: The number of deleted nodes.
        """
        logger.info(
            "Deleting first encountered node of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.node_filters(filters=filters)

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        result, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                WHERE {cls._query_builder.query['where']}
                WITH DISTINCT n
                LIMIT 1
                DETACH DELETE n
                RETURN count(n)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(result) == 0 or len(result[0]) == 0 or result[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Deleted %s nodes", result[0][0])
        if result[0][0] == 0 and raise_on_empty:
            raise NoResultFound(filters)
        return result[0][0]

    @classmethod
    @hooks
    async def delete_many(
        cls: Type[T],
        filters: Optional[NodeFilters] = None,
        batch_manager: Optional[BatchManager] = None
    ) -> int:
        """
        Finds all nodes that match `filters` and deletes them.

        Args:
            filters (NodeFilters, optional): The filters to apply to the query. Defaults to `None`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Returns:
            int: The number of deleted nodes.
        """
        logger.info("Deleting all nodes of model %s matching filters %s", cls.__name__, filters)
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.node_filters(filters=filters)

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                DETACH DELETE n
                RETURN count(n)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Deleted %s nodes", len(results))
        return results[0][0]

    @classmethod
    @hooks
    async def count(
        cls: Type[T],
        filters: Optional[NodeFilters] = None,
        batch_manager: Optional[BatchManager] = None
    ) -> int:
        """
        Counts all nodes which match the provided `filters` parameter.

        Args:
            filters (NodeFilters, optional): The filters to apply to the query. Defaults to `None`.
            batch_manager (BatchManager, optional): The batch manager to use for asyncio-safe concurrent batch queries.
                Defaults to `None`.

        Returns:
            int: The number of nodes matched by the query.
        """
        logger.info(
            "Getting count of nodes of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.node_filters(filters=filters)

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.node_match(list(cls._settings.labels))}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                RETURN count(n)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        return results[0][0]

    @classmethod
    def _register_relationship_properties(cls) -> None:
        """
        Registers the relationship-properties defined on the model for later use.
        """
        logger.debug("Collecting relationship properties for model %s", cls.__name__)
        for property_name, value in get_model_fields(cls).items():
            # Check if value is None here to prevent breaking logic if property_name is of type None
            if get_field_type(value) is not None and hasattr(get_field_type(value), "_build_property"):
                cls._relationship_properties.add(property_name)

    def _deflate(self) -> Dict[str, Any]:
        """
        Deflates the current model instance into a python dictionary which can be stored in Neo4j.

        Returns:
            Dict[str, Any]: The deflated model instance.
        """
        logger.debug("Deflating model %s to storable dictionary", self)
        deflated: Dict[str, Any] = json.loads(
            get_model_dump_json(self, exclude={*self._relationship_properties, "_settings"})
        )

        return super()._deflate(deflated=deflated)

    @classmethod
    def _inflate(cls: Type[T], graph_entity: Node) -> T:
        """
        Inflates a node instance into a instance of the current model.

        Args:
            node (Node): Node to inflate.

        Raises:
            InflationFailure: If inflating the node fails.

        Returns:
            T: A new instance of the current model with the properties from the node instance.
        """
        inflated = super()._inflate(graph_entity=graph_entity)

        for relationship_property in cls._relationship_properties:
            inflated.pop(relationship_property, None)

        instance = cls(**inflated)
        setattr(instance, "_element_id", graph_entity._element_id)
        setattr(instance, "_id", graph_entity._id)
        return instance

    @classmethod
    def _build_auto_fetch(
        cls, nodes_to_fetch: Optional[List[Union[str, Type["NodeModel"]]]] = None, ref: str = "n"
    ) -> Tuple[List[str], List[str]]:
        """
        Builds the auto-fetch query for the instance.

        Args:
            nodes_to_fetch (List[Union[str, Type["NodeModel"]]] | None): The nodes to fetch. Can contain the actual
                model of the node or the model name as a string. If `None`, all nodes will be fetched. Defaults to
                `None`.
            ref (str, optional): The reference to use for the node. Defaults to "n".

        Returns:
            Tuple[List[str], List[str]]: The `MATCH` and `RETURN` queries.
        """
        match_queries: List[str] = []
        return_queries: List[str] = []
        node_model_names: List[str] = []

        if nodes_to_fetch is not None:
            for node in nodes_to_fetch:
                if isinstance(node, str):
                    node_model_names.append(node)
                else:
                    node_model_names.append(node.__name__)

        logger.debug("Building node match queries for auto-fetch")
        for defined_relationship in cls._relationship_properties:
            relationship_type: Optional[str] = None
            end_node_labels: Optional[List[str]] = None
            relationship_property = cast(RelationshipProperty, get_model_fields(cls)[defined_relationship].default)
            direction = getattr(relationship_property, "_direction")

            if (
                nodes_to_fetch is not None
                and getattr(relationship_property, "_target_model_name") not in node_model_names
            ):
                continue

            for model in cls._client.models:
                if model.__name__ == getattr(relationship_property, "_relationship_model_name", None):
                    relationship_type = cast(RelationshipModelSettings, model._settings).type
                elif model.__name__ == getattr(relationship_property, "_target_model_name", None):
                    end_node_labels = list(cast(NodeModelSettings, model._settings).labels)
                elif relationship_type is not None and end_node_labels is not None:
                    break

            if relationship_type is None or end_node_labels is None:
                raise UnregisteredModel(cls.__name__)

            return_queries.append(defined_relationship)
            match_queries.append(
                cls._query_builder.relationship_match(
                    ref=None,
                    type_=relationship_type,
                    start_node_ref=ref,
                    direction=direction,
                    end_node_ref=defined_relationship,
                    end_node_labels=end_node_labels,
                )
            )

        return match_queries, return_queries
