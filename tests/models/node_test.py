# pylint: disable=missing-class-docstring, unused-import, redefined-outer-name, unused-argument, line-too-long

import json
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import patch

import neo4j.graph
import pytest
from pydantic import BaseModel

from pyneo4j_ogm.clients.neo4j import Neo4jClient
from pyneo4j_ogm.exceptions import (
    DeflationError,
    EntityAlreadyCreatedError,
    EntityDestroyedError,
    EntityNotFoundError,
    EntityNotHydratedError,
)
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
from tests.helpers.actions import get_async_func, get_sync_func


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


class NullableNode(Node):
    optional_str: Optional[str]

    ogm_config = {"labels": "NullableNode"}


async def prepare_simple_node(client) -> SimpleNode:
    node = SimpleNode(
        str_field="my_str",
        bool_field=True,
        int_field=4,
        float_field=1.243,
        tuple_field=tuple([1, 2, 3]),
        list_field=[1, 2, 3],
        set_field={1, 2, 3},
    )

    query = await client.run(
        "CREATE (n:SimpleNode {str_field: $str_field, bool_field: $bool_field, int_field: $int_field, float_field: $float_field, tuple_field: $tuple_field, list_field: $list_field, set_field: $set_field}) RETURN n",
        {
            "str_field": node.str_field,
            "bool_field": node.bool_field,
            "int_field": node.int_field,
            "float_field": node.float_field,
            "tuple_field": list(node.tuple_field),
            "list_field": node.list_field,
            "set_field": list(node.set_field),
        },
    )
    result = await query.values()
    await query.consume()

    setattr(node, "_element_id", result[0][0].element_id)
    setattr(node, "_id", result[0][0].id)
    setattr(node, "_graph", result[0][0].graph)
    setattr(node, "_state_snapshot", node.model_copy())

    return node


class TestSerialization:
    class EmptyNode(Node):
        pass

    def test_includes_id_and_element_id_in_serialization(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)
        setattr(node, "_element_id", "1")

        serialized = node.model_dump()

        assert len(serialized.keys()) == 2
        assert "id" in serialized
        assert serialized["id"] == 1
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_defined_in_exclude_option(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)
        setattr(node, "_element_id", "1")

        serialized = node.model_dump(exclude={"id"})

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_not_defined_in_include_option(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)
        setattr(node, "_element_id", "1")

        serialized = node.model_dump(include={"element_id"})

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_is_none_and_exclude_non_option(self):
        node = self.EmptyNode()
        setattr(node, "_element_id", "1")

        serialized = node.model_dump(exclude_none=True)

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_element_id_if_defined_in_exclude_option(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)
        setattr(node, "_element_id", "1")

        serialized = node.model_dump(exclude={"element_id"})

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1

    def test_excludes_element_id_if_not_defined_in_include_option(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)
        setattr(node, "_element_id", "1")

        serialized = node.model_dump(include={"id"})

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1

    def test_excludes_element_id_if_is_none_and_exclude_non_option(self):
        node = self.EmptyNode()
        setattr(node, "_id", 1)

        serialized = node.model_dump(exclude_none=True)

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1


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


class TestUpdate:
    async def prepare_node_with_actions(self, client, node):
        query = await client.run("CREATE (n:NodeWithActions) RETURN n")
        result = await query.values()
        await query.consume()

        setattr(node, "_element_id", result[0][0].element_id)
        setattr(node, "_id", result[0][0].id)
        setattr(node, "_graph", result[0][0].graph)

    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.UPDATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.UPDATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.UPDATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.UPDATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_raises_when_not_hydrated(self, neo4j_client):
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

        with pytest.raises(EntityNotHydratedError):
            await node.update()

    @pytest.mark.neo4j
    async def test_raises_when_destroyed(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleNode)

        node = await prepare_simple_node(neo4j_session)
        setattr(node, "_destroyed", True)

        with pytest.raises(EntityDestroyedError):
            await node.update()

    @pytest.mark.neo4j
    async def test_raises_when_no_result_returned(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleNode)
        node = await prepare_simple_node(neo4j_session)

        with patch.object(neo4j_client, "cypher", return_value=([], [])):
            with pytest.raises(EntityNotFoundError):
                node.str_field = "updated_str"
                await node.update()

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_update_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleNode)
            node = await prepare_simple_node(neo4j_session)

            node.str_field = "updated_str_field"
            node.bool_field = False
            node.tuple_field = tuple([5, 6, 7])
            node.list_field = [5, 6, 7]

            assert node.modified_fields == {"str_field", "bool_field", "tuple_field", "list_field"}

            await node.update()

            query = await neo4j_session.run(
                "MATCH (n:SimpleNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
            )
            result = await query.values()
            await query.consume()

            graph_properties = dict(result[0][0])
            assert graph_properties["str_field"] == "updated_str_field"
            assert not graph_properties["bool_field"]
            assert graph_properties["int_field"] == 4
            assert graph_properties["float_field"] == 1.243
            assert tuple(graph_properties["tuple_field"]) == tuple([5, 6, 7])
            assert graph_properties["list_field"] == [5, 6, 7]
            assert set(graph_properties["set_field"]) == set([1, 2, 3])

        async def test_skips_if_nothing_modified(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleNode)
            node = await prepare_simple_node(neo4j_session)

            with patch.object(neo4j_client, "cypher", wraps=neo4j_client.cypher) as spy:
                await node.update()

                assert spy.call_count == 0

            query = await neo4j_session.run(
                "MATCH (n:SimpleNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
            )
            result = await query.values()
            await query.consume()

            graph_properties = dict(result[0][0])
            assert graph_properties["str_field"] == "my_str"
            assert graph_properties["bool_field"]
            assert graph_properties["int_field"] == 4
            assert graph_properties["float_field"] == 1.243
            assert tuple(graph_properties["tuple_field"]) == tuple([1, 2, 3])
            assert graph_properties["list_field"] == [1, 2, 3]
            assert set(graph_properties["set_field"]) == set([1, 2, 3])

    @pytest.mark.memgraph
    class TestWithMemgraphClient:
        async def test_update_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleNode)
            node = await prepare_simple_node(memgraph_session)

            node.str_field = "updated_str_field"
            node.bool_field = False
            node.tuple_field = tuple([5, 6, 7])
            node.list_field = [5, 6, 7]

            assert node.modified_fields == {"str_field", "bool_field", "tuple_field", "list_field"}

            await node.update()

            query = await memgraph_session.run("MATCH (n:SimpleNode) WHERE id(n) = $id RETURN n", {"id": node.id})
            result = await query.values()
            await query.consume()

            graph_properties = dict(result[0][0])
            assert graph_properties["str_field"] == "updated_str_field"
            assert not graph_properties["bool_field"]
            assert graph_properties["int_field"] == 4
            assert graph_properties["float_field"] == 1.243
            assert tuple(graph_properties["tuple_field"]) == tuple([5, 6, 7])
            assert graph_properties["list_field"] == [5, 6, 7]
            assert set(graph_properties["set_field"]) == set([1, 2, 3])

        async def test_skips_if_nothing_modified(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleNode)
            node = await prepare_simple_node(memgraph_session)

            with patch.object(memgraph_client, "cypher", wraps=memgraph_client.cypher) as spy:
                await node.update()

                assert spy.call_count == 0

            query = await memgraph_session.run("MATCH (n:SimpleNode) WHERE id(n) = $id RETURN n", {"id": node.id})
            result = await query.values()
            await query.consume()

            graph_properties = dict(result[0][0])
            assert graph_properties["str_field"] == "my_str"
            assert graph_properties["bool_field"]
            assert graph_properties["int_field"] == 4
            assert graph_properties["float_field"] == 1.243
            assert tuple(graph_properties["tuple_field"]) == tuple([1, 2, 3])
            assert graph_properties["list_field"] == [1, 2, 3]
            assert set(graph_properties["set_field"]) == set([1, 2, 3])


class TestCreate:
    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.CREATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await node.create()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.CREATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await node.create()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.CREATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await node.create()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.CREATE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await node.create()

        assert get_count() == 2

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

        async def test_node_with_optional_field(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(NullableNode)

            node = NullableNode(optional_str=None)
            await node.create()

            query = await neo4j_session.run(
                "MATCH (n:NullableNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": node.element_id}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert len(properties.keys()) == 0

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

        async def test_node_with_optional_field(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(NullableNode)

            node = NullableNode(optional_str=None)
            await node.create()

            query = await memgraph_session.run("MATCH (n:NullableNode) WHERE id(n) = $id RETURN n", {"id": node.id})
            result = await query.values()
            await query.consume()

            assert len(result) == 1
            assert isinstance(result[0][0], neo4j.graph.Node)

            properties = dict(result[0][0])
            assert result[0][0].id == node.id
            assert result[0][0].element_id == node.element_id
            assert len(properties.keys()) == 0

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


class TestDelete:
    async def prepare_node_with_actions(self, client, node):
        query = await client.run("CREATE (n:NodeWithActions) RETURN n")
        result = await query.values()
        await query.consume()

        setattr(node, "_element_id", result[0][0].element_id)
        setattr(node, "_id", result[0][0].id)
        setattr(node, "_graph", result[0][0].graph)

    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.DELETE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.DELETE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.DELETE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.DELETE: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_raises_when_not_hydrated(self, neo4j_client):
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

        with pytest.raises(EntityNotHydratedError):
            await node.delete()

    @pytest.mark.neo4j
    async def test_raises_when_destroyed(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleNode)

        node = await prepare_simple_node(neo4j_session)
        setattr(node, "_destroyed", True)

        with pytest.raises(EntityDestroyedError):
            await node.delete()

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_delete_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleNode)
            node = await prepare_simple_node(neo4j_session)
            element_id = node.element_id

            await node.delete()

            query = await neo4j_session.run(
                "MATCH (n:SimpleNode) WHERE elementId(n) = $element_id RETURN n", {"element_id": element_id}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

    @pytest.mark.memgraph
    class TestWithMemgraphClient:
        async def test_update_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleNode)
            node = await prepare_simple_node(memgraph_session)
            id_ = node.id

            await node.delete()

            query = await memgraph_session.run("MATCH (n:SimpleNode) WHERE id(n) = $id RETURN n", {"id": id_})
            result = await query.values()
            await query.consume()

            assert len(result) == 0


class TestRefresh:
    async def prepare_node_with_actions(self, client, node):
        query = await client.run("CREATE (n:NodeWithActions) RETURN n")
        result = await query.values()
        await query.consume()

        setattr(node, "_element_id", result[0][0].element_id)
        setattr(node, "_id", result[0][0].id)
        setattr(node, "_graph", result[0][0].graph)

    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.REFRESH: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.refresh()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"before_actions": {ActionType.REFRESH: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.refresh()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.REFRESH: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.refresh()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class NodeWithActions(Node):
            ogm_config = {"after_actions": {ActionType.REFRESH: [mock_func, mock_func]}, "labels": ["NodeWithActions"]}

        await neo4j_client.register_models(NodeWithActions)

        node = NodeWithActions()
        await self.prepare_node_with_actions(neo4j_session, node)
        await node.refresh()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_raises_when_not_hydrated(self, neo4j_client):
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

        with pytest.raises(EntityNotHydratedError):
            await node.refresh()

    @pytest.mark.neo4j
    async def test_raises_when_destroyed(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleNode)

        node = await prepare_simple_node(neo4j_session)
        setattr(node, "_destroyed", True)

        with pytest.raises(EntityDestroyedError):
            await node.refresh()

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_refresh_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleNode)
            node = await prepare_simple_node(neo4j_session)
            element_id = node.element_id

            node.str_field = "something else"
            assert len(node.modified_fields) == 1
            assert node.modified_fields == {"str_field"}

            query = await neo4j_session.run(
                "MATCH (n:SimpleNode) WHERE elementId(n) = $element_id SET n.str_field = $str_field",
                {"element_id": element_id, "str_field": "updated"},
            )
            await query.values()
            await query.consume()

            await node.refresh()

            assert node.str_field == "updated"
            assert len(node.modified_fields) == 0

    @pytest.mark.memgraph
    class TestWithMemgraphClient:
        async def test_refresh_node(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleNode)
            node = await prepare_simple_node(memgraph_session)
            id_ = node.id

            node.str_field = "something else"
            assert len(node.modified_fields) == 1
            assert node.modified_fields == {"str_field"}

            query = await memgraph_session.run(
                "MATCH (n:SimpleNode) WHERE id(n) = $id SET n.str_field = $str_field",
                {"id": id_, "str_field": "updated"},
            )
            await query.values()
            await query.consume()

            await node.refresh()

            assert node.str_field == "updated"
            assert len(node.modified_fields) == 0
