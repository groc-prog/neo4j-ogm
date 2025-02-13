from typing import Any, Dict, List, Optional, Tuple, Union
from uuid import uuid4

from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.types.graph import RelationshipDirection


class QueryBuilder:
    """
    Class for building common parts of Cypher queries like MATCH patterns.
    """

    @classmethod
    def build_node_pattern(
        cls, ref: Optional[str] = None, labels: Optional[Union[List[str], str]] = None, pipe_chaining: bool = False
    ) -> str:
        """
        Builds a node pattern which can be used with various expressions in Cypher queries.

        Args:
            ref (Optional[str]): The reference to the node used in the query. Defaults to `None`.
            labels (Optional[Union[List[str], str]]): The labels to use for the pattern. Defaults
                to `None`.
            pipe_chaining (bool): Whether to chain labels using `|` instead of `:`. Defaults to `False`.

        Returns:
            str: The generated node pattern.
        """
        normalized_ref = "" if ref is None else ref
        normalized_labels = ""

        if labels is not None:
            normalized = [labels] if isinstance(labels, str) else labels
            normalized_labels = "|".join(normalized) if pipe_chaining else ":".join(normalized)

        if len(normalized_labels) > 0:
            logger.debug("Building node pattern with ref '%s' and labels %s", normalized_ref, normalized_labels)
            pattern = f"({normalized_ref}:{normalized_labels})"
        else:
            logger.debug("Building node pattern with ref '%s' without labels", normalized_ref)
            pattern = f"({normalized_ref})"

        return pattern

    @classmethod
    def build_relationship_pattern(
        cls,
        ref: Optional[str] = None,
        types: Optional[Union[List[str], str]] = None,
        direction: RelationshipDirection = RelationshipDirection.OUTGOING,
        start_node_ref: Optional[str] = None,
        start_node_labels: Optional[Union[List[str], str]] = None,
        end_node_ref: Optional[str] = None,
        end_node_labels: Optional[Union[List[str], str]] = None,
    ) -> str:
        """
        Builds a relationship pattern which can be used with various expressions in Cypher queries.

        Args:
            ref (Optional[str]): The reference to the relationship used in the query. Defaults to `None`.
            types (Optional[Union[List[str], str]]): The types to use for the relationship. If multiple are provided,
                they will be chained together with a `|`. Defaults to `None`.
            direction (RelationshipDirection): The direction of the relationship pattern. Defaults to
                `RelationshipDirection.OUTGOING`.
            start_node_ref (Optional[str]): The reference to the start node used in the query. Defaults to `None`.
            start_node_labels (Optional[Union[List[str], str]]): The labels to use for the start node pattern.
                Defaults to `None`.
            end_node_ref (Optional[str]): The reference to the end node used in the query. Defaults to `None`.
            end_node_labels (Optional[Union[List[str], str]]): The labels to use for the end node pattern.
                Defaults to `None`.

        Returns:
            str: The generated relationship pattern.
        """
        normalized_rel_ref = "" if ref is None else ref
        normalized_types = ""

        if types is not None:
            normalized = [types] if isinstance(types, str) else types
            normalized_types = "|".join(normalized)

        if len(normalized_types) > 0:
            partial_rel_pattern = f"[{normalized_rel_ref}:{normalized_types}]"
        else:
            partial_rel_pattern = f"[{normalized_rel_ref}]"

        start_node_pattern = cls.build_node_pattern(start_node_ref, start_node_labels)
        end_node_pattern = cls.build_node_pattern(end_node_ref, end_node_labels)

        match direction:
            case RelationshipDirection.INCOMING:
                rel_pattern = f"<-{partial_rel_pattern}-"
            case RelationshipDirection.OUTGOING:
                rel_pattern = f"-{partial_rel_pattern}->"
            case RelationshipDirection.BOTH:
                rel_pattern = f"-{partial_rel_pattern}-"

        return f"{start_node_pattern}{rel_pattern}{end_node_pattern}"

    @classmethod
    def build_set_clause(cls, ref: str, properties: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        Builds a `SET` clause for use in cypher queries.

        Args:
            ref (str): The reference to the graph entity used in the SET clause.
            properties (Dict[str, Any]): A dictionary where the key is the property name and the value
                is the value to be set.

        Returns:
            Tuple[str, Dict[str, Any]]: The SET clause and all used placeholders.
        """
        if len(properties) == 0:
            return "", {}

        placeholders: Dict[str, Any] = {}
        expressions: List[str] = []

        for property_name, property_value in properties.items():
            uid = str(uuid4())

            placeholders[uid] = property_value
            expressions.append(f"{ref}.{property_name} = ${uid}")

        return f"SET {', '.join(expressions)}", placeholders
