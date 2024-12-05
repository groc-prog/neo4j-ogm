from typing import List, Optional, Union

from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.types.graph import RelationshipDirection


class QueryBuilder:
    """
    Class for building common parts of Cypher queries like MATCH patterns.
    """

    @classmethod
    def node_pattern(
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
    def relationship_pattern(
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

        start_node_pattern = cls.node_pattern(start_node_ref, start_node_labels)
        end_node_pattern = cls.node_pattern(end_node_ref, end_node_labels)

        match direction:
            case RelationshipDirection.INCOMING:
                rel_pattern = f"<-{partial_rel_pattern}-"
            case RelationshipDirection.OUTGOING:
                rel_pattern = f"-{partial_rel_pattern}->"
            case RelationshipDirection.BOTH:
                rel_pattern = f"-{partial_rel_pattern}-"

        return f"{start_node_pattern}{rel_pattern}{end_node_pattern}"
