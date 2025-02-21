# pylint: disable=missing-class-docstring, unused-import, redefined-outer-name, unused-argument

import json
from types import NoneType
from typing import Any, Dict, List, Set, Tuple

import neo4j.graph
import pytest
from pydantic import BaseModel

from pyneo4j_ogm.clients.neo4j import Neo4jClient
from pyneo4j_ogm.exceptions import DeflationError, EntityAlreadyCreatedError
from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.types.model import ActionType, EagerFetchStrategy
from tests.fixtures.db import (
    Authentication,
    ConnectionString,
    memgraph_client,
    memgraph_session,
    neo4j_client,
    neo4j_session,
)
from tests.fixtures.registry import reset_registry_state


class NotStorable:
    pass


class NestedModel(BaseModel):
    str_field: str


class SimpleNode(Node):
    str_field: str
    bool_field: bool
    int_field: int
    float_field: float
    tuple_field: Tuple[int, ...]
    list_field: List[int]
    set_field: Set[int]

    ogm_config = {"labels": ["SimpleNode"]}


class NestedNode(Node):
    not_nested: str
    nested_once: Dict[str, Any]
    nested_model: NestedModel

    ogm_config = {"labels": ["NestedNode"]}


class NonHomogeneousListNode(Node):
    list_: List[Any]

    ogm_config = {"labels": "NonHomogeneousListNode"}


class NonStorableNode(Node):
    item: NotStorable

    model_config = {"arbitrary_types_allowed": True}
    ogm_config = {"labels": "NonStorableNode"}


class TestConfiguration:
    def test_default_labels(self):
        class Developer(Node):
            pass

        assert set(Developer._ogm_config.labels) == set(["Developer"])  # type: ignore

    def test_default_labels_with_multi_word_name(self):
        class DeveloperPerson(Node):
            pass

        assert set(DeveloperPerson._ogm_config.labels) == set(["DeveloperPerson"])  # type: ignore

    def test_labels_inheritance(self):
        class Person(Node):
            pass

        class Developer(Person):
            pass

        class Worker(Person):
            ogm_config = {"labels": {"HardWorking", "Human"}}

        assert set(Person._ogm_config.labels) == set(["Person"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["Developer", "Person"])  # type: ignore
        assert set(Worker._ogm_config.labels) == set(["Person", "HardWorking", "Human"])  # type: ignore

    def test_labels_config(self):
        class Person(Node):
            ogm_config = {"labels": "Worker"}

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore

    def test_labels_inheritance_with_parent_config(self):
        class Person(Node):
            ogm_config = {"labels": "Worker"}

        class Developer(Person):
            pass

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["Developer", "Worker"])  # type: ignore

    def test_labels_inheritance_with_child_config(self):
        class Person(Node):
            pass

        class Developer(Person):
            ogm_config = {"labels": "PythonDeveloper"}

        assert set(Person._ogm_config.labels) == set(["Person"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["PythonDeveloper", "Person"])  # type: ignore

    def test_labels_as_str(self):
        class Person(Node):
            ogm_config = {"labels": "Worker"}

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore

    def test_labels_as_list(self):
        class Person(Node):
            ogm_config = {"labels": ["Worker", "HardWorking"]}

        assert set(Person._ogm_config.labels) == set(["Worker", "HardWorking"])  # type: ignore

    def test_labels_as_set(self):
        class Person(Node):
            ogm_config = {"labels": {"Worker", "HardWorking"}}

        assert set(Person._ogm_config.labels) == set(["Worker", "HardWorking"])  # type: ignore

    def test_single_pre_action(self):
        def action_func(ctx, *args, **kwargs):
            pass

        class Person(Node):
            ogm_config = {"before_actions": {ActionType.CREATE: action_func}}

        assert Person._ogm_config.before_actions == {ActionType.CREATE: [action_func]}  # type: ignore

    def test_multiple_pre_action(self):
        def action_func_one(ctx, *args, **kwargs):
            pass

        def action_func_two(ctx, *args, **kwargs):
            pass

        class Person(Node):
            ogm_config = {"before_actions": {ActionType.CREATE: [action_func_one, action_func_two]}}

        assert Person._ogm_config.before_actions == {ActionType.CREATE: [action_func_one, action_func_two]}  # type: ignore

    def test_single_post_action(self):
        def action_func(ctx, *args, **kwargs):
            pass

        class Person(Node):
            ogm_config = {"after_actions": {ActionType.CREATE: action_func}}

        assert Person._ogm_config.after_actions == {ActionType.CREATE: [action_func]}  # type: ignore

    def test_multiple_post_action(self):
        def action_func_one(ctx, *args, **kwargs):
            pass

        def action_func_two(ctx, *args, **kwargs):
            pass

        class Person(Node):
            ogm_config = {"after_actions": {ActionType.CREATE: [action_func_one, action_func_two]}}

        assert Person._ogm_config.after_actions == {ActionType.CREATE: [action_func_one, action_func_two]}  # type: ignore

    def test_primitive_config_options(self):
        class Person(Node):
            ogm_config = {
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        assert Person._ogm_config.skip_constraint_creation is True  # type: ignore
        assert Person._ogm_config.skip_index_creation is True  # type: ignore
        assert Person._ogm_config.eager_fetch is True  # type: ignore
        assert Person._ogm_config.eager_fetch_strategy == EagerFetchStrategy.AS_SPLIT_QUERY  # type: ignore


class TestCreate:
    @pytest.mark.neo4j
    async def test_raises_if_already_hydrated(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleNode)

        node = SimpleNode(
            str_field="my_str",
            bool_field=True,
            int_field=4,
            float_field=1.243,
            tuple_field=tuple([1, 2, 3]),
            list_field=[1, 2, 3],
            set_field={1, 2, 3},
        )
        await node.create()

        with pytest.raises(EntityAlreadyCreatedError):
            await node.create()

        query = await neo4j_session.run(
            "MATCH (n:NestedNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
        )
        result = await query.values()
        await query.consume()

        assert len(result) == 0

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_create_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleNode)

            query = await neo4j_session.run("MATCH (n:SimpleNode) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 0

            node = SimpleNode(
                str_field="my_str",
                bool_field=True,
                int_field=4,
                float_field=1.243,
                tuple_field=tuple([1, 2, 3]),
                list_field=[1, 2, 3],
                set_field={1, 2, 3},
            )

            assert node.element_id is None
            assert node.id is None
            assert node.graph is None
            assert len(node.modified_fields) == 0

            await node.create()

            assert node.element_id is not None
            assert node.id is not None
            assert node.graph is not None
            assert len(node.modified_fields) == 0

            query = await neo4j_session.run("MATCH (n:SimpleNode) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["str_field"] == node.str_field
            assert properties["bool_field"] == node.bool_field
            assert properties["int_field"] == node.int_field
            assert properties["float_field"] == node.float_field
            assert properties["tuple_field"] == list(node.tuple_field)
            assert properties["list_field"] == node.list_field
            assert sorted(properties["set_field"]) == sorted(list(node.set_field))

        async def test_create_nested_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NestedNode)

            node = NestedNode(
                not_nested="not_nested", nested_once={"nested": True}, nested_model=NestedModel(str_field="str_field")
            )
            await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NestedNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["not_nested"] == "not_nested"
            assert properties["nested_once"] == json.dumps(node.nested_once)
            assert properties["nested_model"] == json.dumps(node.nested_model.model_dump())

        async def test_create_raises_if_nested_properties_not_allowed(self, neo4j_session):
            client = Neo4jClient()
            await client.connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, allow_nested_properties=False
            )
            await client.register_models(NestedNode)

            node = NestedNode(
                not_nested="not_nested", nested_once={"nested": True}, nested_model=NestedModel(str_field="str_field")
            )

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NestedNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

        async def test_create_raises_if_list_is_not_homogenous(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NonHomogeneousListNode)

            node = NonHomogeneousListNode(list_=[1, "foo", True])

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NonHomogeneousListNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

        async def test_create_attempts_to_stringify_forbidden_list_items(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NonHomogeneousListNode)

            node = NonHomogeneousListNode(list_=[{"value": "stringified"}])
            await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NonHomogeneousListNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["list_"] == [json.dumps(node.list_[0])]

        async def test_raises_when_stringify_forbidden_list_items_fails(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NonHomogeneousListNode)

            node = NonHomogeneousListNode(list_=[{"item": NotStorable()}])

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NonHomogeneousListNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

        async def test_raises_when_stringify_properties_are_forbidden_in_lists(self, neo4j_session):
            client = Neo4jClient()
            await client.connect(
                ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value, allow_nested_properties=False
            )
            await client.register_models(NonHomogeneousListNode)

            node = NonHomogeneousListNode(list_=[{"item": True}])

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NonHomogeneousListNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

        async def test_raises_when_property_is_not_storable_type(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NonStorableNode)

            node = NonStorableNode(item=NotStorable())

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NonStorableNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

        async def test_raises_when_stringify_nested_property_fails(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NestedNode)

            node = NestedNode(
                not_nested="not_nested",
                nested_once={"nested": NotStorable()},
                nested_model=NestedModel(str_field="str_field"),
            )

            with pytest.raises(DeflationError):
                await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NestedNode) WHERE elementId(n) = $element_id RETURN n",
                {"element_id": node.element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

    @pytest.mark.memgraph
    class TestWithMemgraphClient:
        async def test_create_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleNode)

            query = await memgraph_session.run("MATCH (n:SimpleNode) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 0

            node = SimpleNode(
                str_field="my_str",
                bool_field=True,
                int_field=4,
                float_field=1.243,
                tuple_field=tuple([1, 2, 3]),
                list_field=[1, 2, 3],
                set_field={1, 2, 3},
            )

            assert node.element_id is None
            assert node.id is None
            assert node.graph is None
            assert len(node.modified_fields) == 0

            await node.create()

            assert node.element_id is not None
            assert node.id is not None
            assert node.graph is not None
            assert len(node.modified_fields) == 0

            query = await memgraph_session.run("MATCH (n:SimpleNode) RETURN n")
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["str_field"] == node.str_field
            assert properties["bool_field"] == node.bool_field
            assert properties["int_field"] == node.int_field
            assert properties["float_field"] == node.float_field
            assert properties["tuple_field"] == list(node.tuple_field)
            assert properties["list_field"] == node.list_field
            assert sorted(properties["set_field"]) == sorted(list(node.set_field))

        async def test_create_nested_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(NestedNode)

            node = NestedNode(
                not_nested="not_nested", nested_once={"nested": True}, nested_model=NestedModel(str_field="str_field")
            )
            await node.create()

            query = await memgraph_session.run("MATCH (n:NestedNode) WHERE id(n) = $id RETURN n", {"id": node.id})
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["not_nested"] == "not_nested"
            assert properties["nested_once"] == node.nested_once
            assert properties["nested_model"] == node.nested_model.model_dump()

        async def test_create_non_homogenous_list_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(NonHomogeneousListNode)

            node = NonHomogeneousListNode(list_=[1, "foo", True, {"value": "storable"}])
            await node.create()

            query = await memgraph_session.run(
                "MATCH (n:NonHomogeneousListNode) WHERE id(n) = $id RETURN n", {"id": node.id}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert properties["list_"] == node.list_

        async def test_raises_when_property_is_not_storable_type(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(NonStorableNode)

            node = NonStorableNode(item=NotStorable())

            with pytest.raises(DeflationError):
                await node.create()

            query = await memgraph_session.run("MATCH (n:NonStorableNode) WHERE id(n) = $id RETURN n", {"id": node.id})
            result = await query.values()
            await query.consume()

            assert len(result) == 0
