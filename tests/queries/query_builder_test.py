# pylint: disable=missing-class-docstring

from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import RelationshipDirection


class TestNodePattern:
    def test_empty_pattern(self):
        pattern = QueryBuilder.node_pattern()

        assert pattern == "()"

    def test_pattern_with_ref(self):
        pattern = QueryBuilder.node_pattern("n")

        assert pattern == "(n)"

    def test_pattern_with_single_label(self):
        pattern = QueryBuilder.node_pattern(labels="Person")

        assert pattern == "(:Person)"

    def test_pattern_with_single_label_and_ref(self):
        pattern = QueryBuilder.node_pattern("n", "Person")

        assert pattern == "(n:Person)"

    def test_pattern_with_multiple_label(self):
        pattern = QueryBuilder.node_pattern(labels=["Person", "Worker", "Retail"])

        assert pattern == "(:Person:Worker:Retail)"

    def test_pattern_with_multiple_label_with_ref(self):
        pattern = QueryBuilder.node_pattern("n", ["Person", "Worker", "Retail"])

        assert pattern == "(n:Person:Worker:Retail)"


class TestRelationshipPattern:
    def test_empty_pattern(self):
        pattern = QueryBuilder.relationship_pattern()

        assert pattern == "()-[]->()"

    def test_pattern_with_relationship_ref(self):
        pattern = QueryBuilder.relationship_pattern("r")

        assert pattern == "()-[r]->()"

    def test_pattern_with_relationship_type(self):
        pattern = QueryBuilder.relationship_pattern(types="LOVES")

        assert pattern == "()-[:LOVES]->()"

    def test_pattern_with_multiple_relationship_type(self):
        pattern = QueryBuilder.relationship_pattern(types=["LOVES", "HATES"])

        assert pattern == "()-[:LOVES|HATES]->()"

    def test_pattern_with_incoming_direction(self):
        pattern = QueryBuilder.relationship_pattern(direction=RelationshipDirection.INCOMING)

        assert pattern == "()<-[]-()"

    def test_pattern_with_outgoing_direction(self):
        pattern = QueryBuilder.relationship_pattern(direction=RelationshipDirection.OUTGOING)

        assert pattern == "()-[]->()"

    def test_pattern_with_no_direction(self):
        pattern = QueryBuilder.relationship_pattern(direction=RelationshipDirection.BOTH)

        assert pattern == "()-[]-()"

    def test_pattern_with_ref_and_type(self):
        pattern = QueryBuilder.relationship_pattern("r", "LOVES")

        assert pattern == "()-[r:LOVES]->()"

    def test_pattern_with_start_node_ref(self):
        pattern = QueryBuilder.relationship_pattern(start_node_ref="n", start_node_labels=["Person", "Worker"])

        assert pattern == "(n:Person:Worker)-[]->()"

    def test_pattern_with_end_node_ref(self):
        pattern = QueryBuilder.relationship_pattern(end_node_ref="n", end_node_labels=["Person", "Worker"])

        assert pattern == "()-[]->(n:Person:Worker)"
