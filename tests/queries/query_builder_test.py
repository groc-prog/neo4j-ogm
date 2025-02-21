# pylint: disable=missing-class-docstring, unused-import

from unittest.mock import patch
from uuid import UUID

from pyneo4j_ogm.clients.memgraph import MemgraphClient
from pyneo4j_ogm.clients.neo4j import Neo4jClient
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.registry import Registry
from pyneo4j_ogm.types.graph import RelationshipDirection
from tests.fixtures.registry import reset_registry_state


class TestNodePattern:
    def test_empty_pattern(self):
        pattern = QueryBuilder.build_node_pattern()

        assert pattern == "()"

    def test_pattern_with_ref(self):
        pattern = QueryBuilder.build_node_pattern("n")

        assert pattern == "(n)"

    def test_pattern_with_single_label(self):
        pattern = QueryBuilder.build_node_pattern(labels="Person")

        assert pattern == "(:Person)"

    def test_pattern_with_single_label_and_ref(self):
        pattern = QueryBuilder.build_node_pattern("n", "Person")

        assert pattern == "(n:Person)"

    def test_pattern_with_multiple_label(self):
        pattern = QueryBuilder.build_node_pattern(labels=["Person", "Worker", "Retail"])

        assert pattern == "(:Person:Worker:Retail)"

    def test_pattern_with_multiple_label_pipe_chaining(self):
        pattern = QueryBuilder.build_node_pattern(labels=["Person", "Worker", "Retail"], pipe_chaining=True)

        assert pattern == "(:Person|Worker|Retail)"

    def test_pattern_with_multiple_label_with_ref(self):
        pattern = QueryBuilder.build_node_pattern("n", ["Person", "Worker", "Retail"])

        assert pattern == "(n:Person:Worker:Retail)"


class TestRelationshipPattern:
    def test_empty_pattern(self):
        pattern = QueryBuilder.build_relationship_pattern()

        assert pattern == "()-[]->()"

    def test_pattern_with_relationship_ref(self):
        pattern = QueryBuilder.build_relationship_pattern("r")

        assert pattern == "()-[r]->()"

    def test_pattern_with_relationship_type(self):
        pattern = QueryBuilder.build_relationship_pattern(types="LOVES")

        assert pattern == "()-[:LOVES]->()"

    def test_pattern_with_multiple_relationship_type(self):
        pattern = QueryBuilder.build_relationship_pattern(types=["LOVES", "HATES"])

        assert pattern == "()-[:LOVES|HATES]->()"

    def test_pattern_with_incoming_direction(self):
        pattern = QueryBuilder.build_relationship_pattern(direction=RelationshipDirection.INCOMING)

        assert pattern == "()<-[]-()"

    def test_pattern_with_outgoing_direction(self):
        pattern = QueryBuilder.build_relationship_pattern(direction=RelationshipDirection.OUTGOING)

        assert pattern == "()-[]->()"

    def test_pattern_with_no_direction(self):
        pattern = QueryBuilder.build_relationship_pattern(direction=RelationshipDirection.BOTH)

        assert pattern == "()-[]-()"

    def test_pattern_with_ref_and_type(self):
        pattern = QueryBuilder.build_relationship_pattern("r", "LOVES")

        assert pattern == "()-[r:LOVES]->()"

    def test_pattern_with_start_node_ref(self):
        pattern = QueryBuilder.build_relationship_pattern(start_node_ref="n", start_node_labels=["Person", "Worker"])

        assert pattern == "(n:Person:Worker)-[]->()"

    def test_pattern_with_end_node_ref(self):
        pattern = QueryBuilder.build_relationship_pattern(end_node_ref="n", end_node_labels=["Person", "Worker"])

        assert pattern == "()-[]->(n:Person:Worker)"


class TestSetClause:
    def test_set_clause_without_properties(self):
        clause, placeholders = QueryBuilder.build_set_clause("n", {})

        assert clause == ""
        assert not placeholders

    def test_set_clause_with_properties(self):
        mock_uuids = [UUID("11111111-1111-1111-1111-111111111111"), UUID("22222222-2222-2222-2222-222222222222")]

        with patch("pyneo4j_ogm.queries.query_builder.uuid4", side_effect=mock_uuids):
            clause, placeholders = QueryBuilder.build_set_clause("n", {"prop1": "val1", "prop2": "val2"})

            assert (
                clause
                == "SET n.prop1 = $v11111111111111111111111111111111, n.prop2 = $v22222222222222222222222222222222"
            )
            assert "v11111111111111111111111111111111" in placeholders
            assert "v22222222222222222222222222222222" in placeholders
            assert placeholders["v11111111111111111111111111111111"] == "val1"
            assert placeholders["v22222222222222222222222222222222"] == "val2"


class TestElementIdPredicate:
    def test_neo4j_client(self):
        registry = Registry()
        setattr(registry._thread_ctx, "active_client", Neo4jClient())

        mock_uuids = [UUID("11111111-1111-1111-1111-111111111111")]

        with patch("pyneo4j_ogm.queries.query_builder.uuid4", side_effect=mock_uuids):
            predicate, parameters = QueryBuilder.build_element_id_predicate(
                "n", "4:03d7c266-8515-4891-94de-e332e899c0b6:15"
            )

            assert parameters["v11111111111111111111111111111111"] == "4:03d7c266-8515-4891-94de-e332e899c0b6:15"
            assert predicate == "elementId(n) = $v11111111111111111111111111111111"

    def test_memgraph_client(self):
        registry = Registry()
        setattr(registry._thread_ctx, "active_client", MemgraphClient())

        mock_uuids = [UUID("11111111-1111-1111-1111-111111111111")]

        with patch("pyneo4j_ogm.queries.query_builder.uuid4", side_effect=mock_uuids):
            predicate, parameters = QueryBuilder.build_element_id_predicate("n", "15")

            assert parameters["v11111111111111111111111111111111"] == 15
            assert predicate == "id(n) = $v11111111111111111111111111111111"
