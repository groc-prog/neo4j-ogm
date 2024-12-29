"""
Holds the base relationship class `RelationshipModel` which is used to define database models for
relationships. It provides base functionality like de-/inflation and validation and methods for interacting with
the database for CRUD operations on relationships.
"""

import json
import re
from functools import wraps
from typing import Any, ClassVar, Dict, List, Optional, Type, TypeVar, Union, cast, TYPE_CHECKING

from neo4j.graph import Node, Relationship
from pydantic import PrivateAttr

from pyneo4j_ogm.core.base import ModelBase, hooks
from pyneo4j_ogm.core.node import NodeModel
from pyneo4j_ogm.exceptions import (
    InstanceDestroyed,
    InstanceNotHydrated,
    InvalidFilters,
    NoResultFound,
    UnexpectedEmptyResult,
)
from pyneo4j_ogm.fields.settings import RelationshipModelSettings
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.pydantic_utils import (
    get_model_dump,
    get_model_dump_json,
    get_model_fields,
)
from pyneo4j_ogm.queries.types import (
    Projection,
    QueryOptions,
    RelationshipFilters,
    RelationshipMatchDirection,
)
if TYPE_CHECKING:
    from pyneo4j_ogm.core.client import BatchManagerConcurrent
else:
    BatchManagerConcurrent = object


T = TypeVar("T", bound="RelationshipModel")


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
        if getattr(self, "_destroyed", False):
            raise InstanceDestroyed()

        if any(
            [
                getattr(self, "_element_id", None) is None,
                getattr(self, "_start_node_element_id", None) is None,
                getattr(self, "_start_node_id", None) is None,
                getattr(self, "_end_node_element_id", None) is None,
                getattr(self, "_end_node_id", None) is None,
            ]
        ):
            raise InstanceNotHydrated()

        return func(self, *args, **kwargs)

    return wrapper


class RelationshipModel(ModelBase[RelationshipModelSettings]):
    """
    Base model for all relationship models. Every relationship model should inherit from this class to have needed base
    functionality like de-/inflation and validation.

    Provides methods for interacting with the database for CRUD operations on relationships. Settings can be defined
    by the inner class `Settings`. Settings control the behavior of the model and are defined in
    `pyneo4j_ogm.fields.settings.RelationshipModelSettings`.
    """

    _settings: RelationshipModelSettings = PrivateAttr()
    _start_node_element_id: Optional[str] = PrivateAttr(default=None)
    _start_node_id: Optional[int] = PrivateAttr(default=None)
    _end_node_element_id: Optional[str] = PrivateAttr(default=None)
    _end_node_id: Optional[int] = PrivateAttr(default=None)
    Settings: ClassVar[Type[RelationshipModelSettings]]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._db_properties = get_model_dump(
            self,
            exclude={
                "element_id",
                "id",
                "start_node_element_id",
                "start_node_id",
                "end_node_element_id",
                "end_node_id",
            },
        )

    def __init_subclass__(cls) -> None:
        if not isinstance(getattr(cls, "_settings", None), RelationshipModelSettings):
            setattr(cls, "_settings", RelationshipModelSettings())

        super().__init_subclass__()

        # Check if node labels is set, if not fall back to model name
        type_ = getattr(cls._settings, "type", None)

        # Check if relationship type is set, else fall back to class name
        if type_ is None:
            logger.debug("No type has been defined for model %s, using model name as type", cls.__name__)
            # Convert class name to upper snake case
            relationship_type = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__)
            setattr(cls._settings, "type", relationship_type.upper())

    def __iter__(self):
        for attr_name, attr_value in super().__iter__():
            yield attr_name, attr_value

        yield "start_node_element_id", self._start_node_element_id
        yield "start_node_id", self._start_node_id
        yield "end_node_element_id", self._end_node_element_id
        yield "end_node_id", self._end_node_id

    @hooks
    @ensure_alive
    async def update(self, batch_manager: Optional[BatchManagerConcurrent] = None) -> None:
        """
        Updates the corresponding relationship in the database with the current instance values.

        Args:
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.
        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        deflated = self._deflate()

        logger.info(
            "Updating relationship %s of model %s with current properties %s",
            self._element_id,
            self.__class__.__name__,
            deflated,
        )
        set_query = ", ".join(
            [
                f"r.{property_name} = ${property_name}"
                for property_name in deflated
                if property_name in self.modified_properties
            ]
        )

        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.relationship_match(type_=self._settings.type)}
                WHERE elementId(r) = $element_id
                {f"SET {set_query}" if set_query != "" else ""}
                RETURN r
            """,
            parameters={
                "element_id": self._element_id,
                **deflated,
            },
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Resetting modified properties")
        self._db_properties = get_model_dump(
            self,
            exclude={
                "element_id",
                "id",
                "start_node_element_id",
                "start_node_id",
                "end_node_element_id",
                "end_node_id",
            },
        )
        logger.debug("Updated relationship %s", self)

    @hooks
    @ensure_alive
    async def delete(self, batch_manager: Optional[BatchManagerConcurrent] = None) -> None:
        """
        Deletes the corresponding relationship in the database and marks this instance as destroyed. If another
        method is called on this instance, an `InstanceDestroyed` will be raised.

        Args:
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        logger.info("Deleting relationship %s", self)
        await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.relationship_match(type_=self._settings.type)}
                WHERE elementId(r) = $element_id
                DELETE r
            """,
            parameters={
                "element_id": self._element_id,
            },
            batch_manager=batch_manager,
        )

        logger.debug("Marking instance as destroyed")
        setattr(self, "_destroyed", True)
        logger.debug("Deleted relationship %s", self._element_id)

    @hooks
    @ensure_alive
    async def refresh(self, batch_manager: Optional[BatchManagerConcurrent] = None) -> None:
        """
        Refreshes the current instance with the values from the database.

        Args:
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
        """
        logger.info("Refreshing relationship %s with values from database", self)
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.relationship_match(type_=self._settings.type)}
                WHERE elementId(r) = $element_id
                RETURN r
            """,
            parameters={"element_id": self._element_id},
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Updating current instance")
        self.__dict__.update(results[0][0].__dict__)
        logger.debug("Refreshed relationship %s", self)

    @hooks
    @ensure_alive
    async def start_node(self, batch_manager: Optional[BatchManagerConcurrent] = None) -> Type[NodeModel]:
        """
        Returns the start node the relationship belongs to.

        Args:
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.

        Returns:
            Type[NodeModel]: A instance of the start node model.
        """
        logger.info("Getting start node %s for relationship %s", self._start_node_element_id, self)
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.relationship_match(type_=self._settings.type, start_node_ref="start")}
                WHERE elementId(r) = $element_id
                RETURN start
            """,
            parameters={
                "element_id": self._element_id,
            },
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        return results[0][0]

    @hooks
    @ensure_alive
    async def end_node(self, batch_manager: Optional[BatchManagerConcurrent] = None) -> Type[NodeModel]:
        """
        Returns the end node the relationship belongs to.

        Args:
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.

        Returns:
            Type[NodeModel]: A instance of the end node model.
        """
        logger.info("Getting end node %s for relationship %s", self._end_node_element_id, self)
        results, _ = await self._client.cypher(
            query=f"""
                MATCH {self._query_builder.relationship_match(type_=self._settings.type, start_node_ref="start", end_node_ref="end")}
                WHERE elementId(r) = $element_id
                RETURN end
            """,
            parameters={
                "element_id": self._element_id,
            },
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        return results[0][0]

    @classmethod
    @hooks
    async def find_one(
        cls: Type[T],
        filters: RelationshipFilters,
        projections: Optional[Projection] = None,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> Optional[Union[T, Dict[str, Any]]]:
        """
        Finds the first relationship that matches `filters` and returns it. If no matching relationship is found,
        `None` is returned instead.

        Args:
            filters (RelationshipFilters): Expressions applied to the query.
            projections (Projection, optional): The properties to project from the relationship. The keys define
                the new keys in the projection and the value defines the model property to be projected. A invalid
                or empty projection will result in the whole model instance being returned. Defaults to `None`.
            raise_on_empty (bool, optional): Whether to raise a `NoResultFound` if no match is found.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            InvalidFilters: If no filters or invalid filters are provided.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            T | Dict[str, Any] | None: A instance of the model or None if no match is found or a dictionary if a
                projection is provided.
        """
        logger.info(
            "Getting first encountered relationship of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.relationship_filters(filters=filters)

        if projections is not None:
            cls._query_builder.build_projections(projections=projections, ref="r")

        projection_query = (
            "RETURN r" if cls._query_builder.query["projections"] == "" else cls._query_builder.query["projections"]
        )

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                WHERE {cls._query_builder.query['where']}
                WITH DISTINCT r
                LIMIT 1
                {projection_query}
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            if raise_on_empty:
                raise NoResultFound(filters)
            return None

        logger.debug("Checking if relationship has to be parsed to instance")
        if isinstance(results[0][0], Relationship):
            return cls._inflate(graph_entity=results[0][0])
        elif isinstance(results[0][0], list):
            return results[0][0][0]

        return results[0][0]

    @classmethod
    @hooks
    async def find_many(
        cls: Type[T],
        filters: Optional[RelationshipFilters] = None,
        projections: Optional[Projection] = None,
        options: Optional[QueryOptions] = None,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> List[Union[T, Dict[str, Any]]]:
        """
        Finds the all relationships that matches `filters` and returns them. If no matching relationships are found,
        an empty list is returned instead.

        Args:
            filters (RelationshipFilters | None, optional): Expressions applied to the query. Defaults to
                `None`.
            projections (Projection, optional): The properties to project from the relationship. The keys define
                the new keys in the projection and the value defines the model property to be projected. A invalid
                or empty projection will result in the whole model instance being returned. Defaults to `None`.
            options (QueryOptions | None, optional): Options for modifying the query result. Defaults to `None`.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Returns:
            List[T | Dict[str, Any]]: A list of model instances or dictionaries if a projection is provided.
        """
        logger.info(
            "Getting relationships of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.relationship_filters(filters=filters)
        if options is not None:
            cls._query_builder.query_options(options=options, ref="r")
        if projections is not None:
            cls._query_builder.build_projections(projections=projections, ref="r")

        instances: List[Union[T, Dict[str, Any]]] = []
        projection_query = (
            "RETURN r" if cls._query_builder.query["projections"] == "" else cls._query_builder.query["projections"]
        )
        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                WITH DISTINCT r
                {cls._query_builder.query['options']}
                {projection_query}
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        # Normalize results to instance classes
        logger.debug("Building instances from query results")
        for result_list in results:
            for result in result_list:
                if result is None:
                    continue

                if isinstance(result, Relationship):
                    instances.append(cls._inflate(graph_entity=result))
                elif isinstance(result, list):
                    instances.extend(result)
                else:
                    instances.append(result)

        return instances

    @classmethod
    @hooks
    async def update_one(
        cls: Type[T],
        update: Dict[str, Any],
        filters: RelationshipFilters,
        new: bool = False,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> Optional[T]:
        """
        Finds the first relationship that matches `filters` and updates it with the values defined by `update`. If
        no match is found, `None` is returned instead.

        Args:
            update (Dict[str, Any]): Values to update the relationship properties with.
            filters (RelationshipFilters): Expressions applied to the query.
            new (bool, optional): Whether to return the updated relationship. By default, the old relationship is
                returned. Defaults to `False`.
            raise_on_empty (bool, optional): Whether to raise a `NoResultFound` if no match is found. Defaults to
                `False`.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            InvalidFilters: If no filters or invalid filters are provided.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            T | None: By default, the old relationship instance is returned. If `new` is set to `True`, the result
                will be the `updated instance`. If no match is found, `None` is returned.
        """
        new_instance: T

        logger.info(
            "Updating first encountered relationship of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        logger.debug(
            "Getting first encountered relationship of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.relationship_filters(filters=filters)

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                WHERE {cls._query_builder.query['where']}
                RETURN DISTINCT r
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
        new_instance = cls(**old_instance._deflate())

        for key, value in update.items():
            if key in get_model_fields(cls):
                setattr(new_instance, key, value)

        setattr(new_instance, "_element_id", getattr(old_instance, "_element_id", None))
        setattr(new_instance, "_start_node_element_id", getattr(old_instance, "_start_node_element_id", None))
        setattr(new_instance, "_start_node_id", getattr(old_instance, "_start_node_id", None))
        setattr(new_instance, "_end_node_element_id", getattr(old_instance, "_end_node_element_id", None))
        setattr(new_instance, "_end_node_id", getattr(old_instance, "_end_node_id", None))

        deflated = new_instance._deflate()
        set_query = ", ".join(
            [
                f"r.{property_name} = ${property_name}"
                for property_name in deflated
                if property_name in new_instance.modified_properties
            ]
        )

        await cls._client.cypher(
            query=f"""
                MATCH {cls._query_builder.relationship_match(type_=new_instance._settings.type)}
                WHERE elementId(r) = $element_id
                {f"SET {set_query}" if set_query != "" else ""}
            """,
            parameters={
                "element_id": new_instance._element_id,
                **deflated,
            },
            batch_manager=batch_manager,
        )

        logger.debug("Successfully updated relationship %s", getattr(new_instance, "_element_id"))
        return new_instance if new else old_instance

    @classmethod
    @hooks
    async def update_many(
        cls: Type[T],
        update: Dict[str, Any],
        filters: Optional[RelationshipFilters] = None,
        new: bool = False,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> List[T]:
        """
        Finds all relationships that match `filters` and updates them with the values defined by `update`.

        Args:
            update (Dict[str, Any]): Values to update the relationship properties with.
            filters (RelationshipFilters): Expressions applied to the query. Defaults to `None`.
            new (bool, optional): Whether to return the updated relationships. By default, the old relationships is
                returned. Defaults to `False`.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Returns:
            List[T]: By default, the old relationship instances are returned. If `new` is set to `True`, the
                result will be the `updated instance`.
        """
        new_instance: T

        logger.info(
            "Updating all relationships of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.relationship_filters(filters=filters)

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        logger.debug(
            "Getting all relationships of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                RETURN DISTINCT r
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        old_instances: List[T] = []

        for result_list in results:
            for result in result_list:
                if result is None:
                    continue

                if isinstance(results[0][0], Relationship):
                    old_instances.append(cls._inflate(graph_entity=results[0][0]))
                else:
                    old_instances.append(result)

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
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                SET {", ".join([f"r.{property_name} = ${property_name}" for property_name in deflated_properties if property_name in update])}
                RETURN DISTINCT r
            """,
            parameters={**deflated_properties, **cls._query_builder.parameters},
            batch_manager=batch_manager,
        )

        logger.debug(
            "Successfully updated %s relationships %s",
            len(old_instances),
            [getattr(instance, "_element_id") for instance in old_instances],
        )
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
        filters: RelationshipFilters,
        raise_on_empty: bool = False,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> int:
        """
        Finds the first relationship that matches `filters` and deletes it. If no match is found, a
        `UnexpectedEmptyResult` is raised.

        Args:
            filters (RelationshipFilters): Expressions applied to the query. Defaults to `None`.
            raise_on_empty (bool, optional): Whether to raise a `NoResultFound` if no match is found. Defaults to
                `False`.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Raises:
            UnexpectedEmptyResult: If the query should return a result but does not.
            NoResultFound: If no match is found and `raise_on_empty` is set to `True`.

        Returns:
            int: The number of deleted relationships.
        """
        logger.info(
            "Deleting first encountered relationship of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        cls._query_builder.relationship_filters(filters=filters)

        if cls._query_builder.query["where"] == "":
            raise InvalidFilters()

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                WHERE {cls._query_builder.query['where']}
                WITH r
                LIMIT 1
                DELETE r
                RETURN count(r)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Deleted %s relationships", results[0][0])
        if results[0][0] == 0 and raise_on_empty:
            raise NoResultFound(filters)
        return results[0][0]

    @classmethod
    @hooks
    async def delete_many(
        cls: Type[T],
        filters: Optional[RelationshipFilters] = None,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> int:
        """
        Finds all relationships that match `filters` and deletes them.

        Args:
            filters (RelationshipFilters): Expressions applied to the query. Defaults to `None`.

        Returns:
            int: The number of deleted relationships.
        """
        logger.info(
            "Deleting all relationships of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.relationship_filters(filters=filters)

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        logger.debug("Getting relationship count matching filters %s", filters)
        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                RETURN count(r)
            """,
            parameters=cls._query_builder.parameters,
            resolve_models=False,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        logger.debug("Deleting relationships")
        await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                DELETE r
                RETURN count(r)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Deleted %s relationships", results[0][0])
        return results[0][0]

    @classmethod
    @hooks
    async def count(
        cls: Type[T],
        filters: Optional[RelationshipFilters] = None,
        batch_manager: Optional[BatchManagerConcurrent] = None,
    ) -> int:
        """
        Counts all relationships which match the provided `filters` parameter.

        Args:
            filters (RelationshipFilters | None, optional): Expressions applied to the query. Defaults
                to `None`.
            batch_manager (BatchManagerConcurrent, optional): The batch manager to use for asyncio-safe concurrent
                batch queries. Defaults to `None`.

        Returns:
            int: The number of relationships matched by the query.
        """
        logger.info(
            "Getting count of relationships of model %s matching filters %s",
            cls.__name__,
            filters,
        )
        cls._query_builder.reset_query()
        if filters is not None:
            cls._query_builder.relationship_filters(filters=filters)

        match_query = cls._query_builder.relationship_match(
            type_=cls._settings.type, direction=RelationshipMatchDirection.OUTGOING
        )

        results, _ = await cls._client.cypher(
            query=f"""
                MATCH {match_query}
                {f"WHERE {cls._query_builder.query['where']}" if cls._query_builder.query['where'] != "" else ""}
                RETURN count(r)
            """,
            parameters=cls._query_builder.parameters,
            batch_manager=batch_manager,
        )

        logger.debug("Checking if query returned a result")
        if len(results) == 0 or len(results[0]) == 0 or results[0][0] is None:
            raise UnexpectedEmptyResult()

        return results[0][0]

    def _deflate(self) -> Dict[str, Any]:
        """
        Deflates the current model instance into a python dictionary which can be stored in Neo4j.

        Returns:
            Dict[str, Any]: The deflated model instance.
        """
        deflated: Dict[str, Any] = json.loads(get_model_dump_json(self, exclude={"_settings"}))

        return super()._deflate(deflated=deflated)

    @classmethod
    def _inflate(cls: Type[T], graph_entity: Relationship) -> T:
        """
        Inflates a relationship instance into a instance of the current model.

        Args:
            relationship (Relationship): Relationship to inflate.

        Raises:
            InflationFailure: If inflating the relationship fails.

        Returns:
            T: A new instance of the current model with the properties from the relationship instance.
        """
        inflated = super()._inflate(graph_entity=graph_entity)
        instance = cls(**inflated)

        setattr(instance, "_element_id", graph_entity.element_id)
        setattr(instance, "_id", graph_entity.id)
        setattr(instance, "_start_node_element_id", cast(Node, graph_entity.start_node).element_id)
        setattr(instance, "_start_node_id", cast(Node, graph_entity.start_node).id)
        setattr(instance, "_end_node_element_id", cast(Node, graph_entity.end_node).element_id)
        setattr(instance, "_end_node_id", cast(Node, graph_entity.end_node).id)

        return instance

    @property
    def start_node_element_id(self) -> Optional[str]:
        """
        Returns the element ID of the start node or None if the instance has not been hydrated.

        Returns:
            str: The element ID of the start node or None if the instance has not been hydrated.
        """
        return self._start_node_element_id

    @property
    def start_node_id(self) -> Optional[int]:
        """
        Returns the ID of the start node or None if the instance has not been hydrated.

        Returns:
            int: The ID of the start node or None if the instance has not been hydrated.
        """
        return self._start_node_id

    @property
    def end_node_element_id(self) -> Optional[str]:
        """
        Returns the element ID of the end node or None if the instance has not been hydrated.

        Returns:
            str: The element ID of the end node or None if the instance has not been hydrated.
        """
        return self._end_node_element_id

    @property
    def end_node_id(self) -> Optional[int]:
        """
        Returns the ID of the end node or None if the instance has not been hydrated.

        Returns:
            int: The ID of the end node or None if the instance has not been hydrated.
        """
        return self._end_node_id
