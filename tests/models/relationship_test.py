# pylint: disable=missing-class-docstring, unused-argument, unused-import, line-too-long, redefined-outer-name


from typing import List, Set, Tuple
from unittest.mock import patch

import pytest

from pyneo4j_ogm.exceptions import (
    EntityDestroyedError,
    EntityNotFoundError,
    EntityNotHydratedError,
)
from pyneo4j_ogm.models.relationship import Relationship
from pyneo4j_ogm.types.model import ActionType, EagerFetchStrategy
from tests.fixtures.db import (
    memgraph_client,
    memgraph_session,
    neo4j_client,
    neo4j_session,
)
from tests.fixtures.registry import reset_registry_state
from tests.helpers.actions import get_async_func, get_sync_func


class SimpleRelationship(Relationship):
    str_field: str
    bool_field: bool
    int_field: int
    float_field: float
    tuple_field: Tuple[int, ...]
    list_field: List[int]
    set_field: Set[int]

    ogm_config = {"type": "SimpleRelationship"}


async def prepare_simple_relationship(client) -> SimpleRelationship:
    relationship = SimpleRelationship(
        str_field="my_str",
        bool_field=True,
        int_field=4,
        float_field=1.243,
        tuple_field=tuple([1, 2, 3]),
        list_field=[1, 2, 3],
        set_field={1, 2, 3},
    )

    query = await client.run(
        "CREATE (n:StartNode)-[r:SIMPLERELATIONSHIP {str_field: $str_field, bool_field: $bool_field, int_field: $int_field, float_field: $float_field, tuple_field: $tuple_field, list_field: $list_field, set_field: $set_field}]->(m:EndNode) RETURN r",
        {
            "str_field": relationship.str_field,
            "bool_field": relationship.bool_field,
            "int_field": relationship.int_field,
            "float_field": relationship.float_field,
            "tuple_field": list(relationship.tuple_field),
            "list_field": relationship.list_field,
            "set_field": list(relationship.set_field),
        },
    )
    result = await query.values()
    await query.consume()

    setattr(relationship, "_element_id", result[0][0].element_id)
    setattr(relationship, "_id", result[0][0].id)
    setattr(relationship, "_graph", result[0][0].graph)
    setattr(relationship, "_state_snapshot", relationship.model_copy())

    return relationship


class TestOGMConfiguration:
    def test_default_type(self):
        class Likes(Relationship):
            pass

        assert Likes._ogm_config.type == "LIKES"  # type: ignore

    def test_custom_type(self):
        class Likes(Relationship):
            ogm_config = {"type": "Loves"}

        assert Likes._ogm_config.type == "LOVES"  # type: ignore

    def test_type_inheritance(self):
        class Likes(Relationship):
            pass

        class Hates(Likes):
            pass

        assert Likes._ogm_config.type == "LIKES"  # type: ignore
        assert Hates._ogm_config.type == "HATES"  # type: ignore

    def test_single_pre_action(self):
        def action_func(ctx, *args, **kwargs):
            pass

        class Likes(Relationship):
            ogm_config = {"before_actions": {ActionType.CREATE: action_func}}

        assert Likes._ogm_config.before_actions == {ActionType.CREATE: [action_func]}  # type: ignore

    def test_multiple_pre_action(self):
        def action_func_one(ctx, *args, **kwargs):
            pass

        def action_func_two(ctx, *args, **kwargs):
            pass

        class Likes(Relationship):
            ogm_config = {"before_actions": {ActionType.CREATE: [action_func_one, action_func_two]}}

        assert Likes._ogm_config.before_actions == {ActionType.CREATE: [action_func_one, action_func_two]}  # type: ignore

    def test_single_post_action(self):
        def action_func(ctx, *args, **kwargs):
            pass

        class Likes(Relationship):
            ogm_config = {"after_actions": {ActionType.CREATE: action_func}}

        assert Likes._ogm_config.after_actions == {ActionType.CREATE: [action_func]}  # type: ignore

    def test_multiple_post_action(self):
        def action_func_one(ctx, *args, **kwargs):
            pass

        def action_func_two(ctx, *args, **kwargs):
            pass

        class Likes(Relationship):
            ogm_config = {"after_actions": {ActionType.CREATE: [action_func_one, action_func_two]}}

        assert Likes._ogm_config.after_actions == {ActionType.CREATE: [action_func_one, action_func_two]}  # type: ignore

    def test_primitive_config_options(self):
        class Likes(Relationship):
            ogm_config = {
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        assert Likes._ogm_config.skip_constraint_creation is True  # type: ignore
        assert Likes._ogm_config.skip_index_creation is True  # type: ignore
        assert Likes._ogm_config.eager_fetch is True  # type: ignore
        assert Likes._ogm_config.eager_fetch_strategy == EagerFetchStrategy.AS_SPLIT_QUERY  # type: ignore


class TestSerialization:
    class EmptyRelationship(Relationship):
        pass

    def test_includes_id_and_element_id_in_serialization(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump()

        assert len(serialized.keys()) == 2
        assert "id" in serialized
        assert serialized["id"] == 1
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_defined_in_exclude_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump(exclude={"id"})

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_not_defined_in_include_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump(include={"element_id"})

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_id_if_is_none_and_exclude_non_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump(exclude_none=True)

        assert len(serialized.keys()) == 1
        assert "id" not in serialized
        assert "element_id" in serialized
        assert serialized["element_id"] == "1"

    def test_excludes_element_id_if_defined_in_exclude_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump(exclude={"element_id"})

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1

    def test_excludes_element_id_if_not_defined_in_include_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)
        setattr(relationship, "_element_id", "1")

        serialized = relationship.model_dump(include={"id"})

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1

    def test_excludes_element_id_if_is_none_and_exclude_non_option(self):
        relationship = self.EmptyRelationship()
        setattr(relationship, "_id", 1)

        serialized = relationship.model_dump(exclude_none=True)

        assert len(serialized.keys()) == 1
        assert "element_id" not in serialized
        assert "id" in serialized
        assert serialized["id"] == 1


class TestUpdate:
    async def prepare_relationship_with_actions(self, client, relationship):
        query = await client.run("CREATE (n:StartNode)-[r:RelationshipWithActions]->(m:EndNode) RETURN r")
        result = await query.values()
        await query.consume()

        setattr(relationship, "_element_id", result[0][0].element_id)
        setattr(relationship, "_id", result[0][0].id)
        setattr(relationship, "_graph", result[0][0].graph)

    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "before_actions": {ActionType.UPDATE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "before_actions": {ActionType.UPDATE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "after_actions": {ActionType.UPDATE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "after_actions": {ActionType.UPDATE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.update()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_raises_when_not_hydrated(self, neo4j_client):
        await neo4j_client.register_models(SimpleRelationship)

        relationship = SimpleRelationship(
            str_field="my_str",
            bool_field=True,
            int_field=4,
            float_field=1.243,
            tuple_field=tuple([1, 2, 3]),
            list_field=[1, 2, 3],
            set_field={1, 2, 3},
        )

        with pytest.raises(EntityNotHydratedError):
            await relationship.update()

    @pytest.mark.neo4j
    async def test_raises_when_destroyed(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleRelationship)

        relationship = await prepare_simple_relationship(neo4j_session)
        setattr(relationship, "_destroyed", True)

        with pytest.raises(EntityDestroyedError):
            await relationship.update()

    @pytest.mark.neo4j
    async def test_raises_when_no_result_returned(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleRelationship)
        relationship = await prepare_simple_relationship(neo4j_session)

        with patch.object(neo4j_client, "cypher", return_value=([], [])):
            with pytest.raises(EntityNotFoundError):
                relationship.str_field = "updated_str"
                await relationship.update()

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_update_node(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(neo4j_session)

            relationship.str_field = "updated_str_field"
            relationship.bool_field = False
            relationship.tuple_field = tuple([5, 6, 7])
            relationship.list_field = [5, 6, 7]

            assert relationship.modified_fields == {"str_field", "bool_field", "tuple_field", "list_field"}

            await relationship.update()

            query = await neo4j_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE elementId(r) = $element_id RETURN r",
                {"element_id": relationship.element_id},
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
            await neo4j_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(neo4j_session)

            with patch.object(neo4j_client, "cypher", wraps=neo4j_client.cypher) as spy:
                await relationship.update()

                assert spy.call_count == 0

            query = await neo4j_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE elementId(r) = $element_id RETURN r",
                {"element_id": relationship.element_id},
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
            await memgraph_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(memgraph_session)

            relationship.str_field = "updated_str_field"
            relationship.bool_field = False
            relationship.tuple_field = tuple([5, 6, 7])
            relationship.list_field = [5, 6, 7]

            assert relationship.modified_fields == {"str_field", "bool_field", "tuple_field", "list_field"}

            await relationship.update()

            query = await memgraph_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE id(r) = $id RETURN r", {"id": relationship.id}
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

        async def test_skips_if_nothing_modified(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(memgraph_session)

            with patch.object(memgraph_client, "cypher", wraps=memgraph_client.cypher) as spy:
                await relationship.update()

                assert spy.call_count == 0

            query = await memgraph_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE id(r) = $id RETURN r", {"id": relationship.id}
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


class TestDelete:
    async def prepare_relationship_with_actions(self, client, relationship):
        query = await client.run("CREATE (n:StartNode)-[r:RelationshipWithActions]->(m:EndNode) RETURN r")
        result = await query.values()
        await query.consume()

        setattr(relationship, "_element_id", result[0][0].element_id)
        setattr(relationship, "_id", result[0][0].id)
        setattr(relationship, "_graph", result[0][0].graph)

    @pytest.mark.neo4j
    async def test_calls_sync_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "before_actions": {ActionType.DELETE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_before_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "before_actions": {ActionType.DELETE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_sync_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_sync_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "after_actions": {ActionType.DELETE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_calls_async_after_actions_with_context(self, neo4j_session, neo4j_client):
        get_count, mock_func = get_async_func()

        class RelationshipWithActions(Relationship):
            ogm_config = {
                "after_actions": {ActionType.DELETE: [mock_func, mock_func]},
                "type": "RelationshipWithActions",
            }

        await neo4j_client.register_models(RelationshipWithActions)

        relationship = RelationshipWithActions()
        await self.prepare_relationship_with_actions(neo4j_session, relationship)
        await relationship.delete()

        assert get_count() == 2

    @pytest.mark.neo4j
    async def test_raises_when_not_hydrated(self, neo4j_client):
        await neo4j_client.register_models(SimpleRelationship)

        relationship = SimpleRelationship(
            str_field="my_str",
            bool_field=True,
            int_field=4,
            float_field=1.243,
            tuple_field=tuple([1, 2, 3]),
            list_field=[1, 2, 3],
            set_field={1, 2, 3},
        )

        with pytest.raises(EntityNotHydratedError):
            await relationship.delete()

    @pytest.mark.neo4j
    async def test_raises_when_destroyed(self, neo4j_session, neo4j_client):
        await neo4j_client.register_models(SimpleRelationship)

        relationship = await prepare_simple_relationship(neo4j_session)
        setattr(relationship, "_destroyed", True)

        with pytest.raises(EntityDestroyedError):
            await relationship.delete()

    @pytest.mark.neo4j
    class TestWithNeo4jClient:
        async def test_delete_relationship(self, neo4j_session, neo4j_client):
            await neo4j_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(neo4j_session)
            element_id = relationship.element_id

            await relationship.delete()

            query = await neo4j_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE elementId(r) = $element_id RETURN r",
                {"element_id": element_id},
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0

    @pytest.mark.memgraph
    class TestWithMemgraphClient:
        async def test_update_relationship(self, memgraph_session, memgraph_client):
            await memgraph_client.register_models(SimpleRelationship)
            relationship = await prepare_simple_relationship(memgraph_session)
            id_ = relationship.id

            await relationship.delete()

            query = await memgraph_session.run(
                "MATCH ()-[r:SIMPLERELATIONSHIP]->() WHERE id(r) = $id RETURN r", {"id": id_}
            )
            result = await query.values()
            await query.consume()

            assert len(result) == 0
