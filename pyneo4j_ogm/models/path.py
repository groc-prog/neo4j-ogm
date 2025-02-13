from typing import Tuple, cast

import neo4j.graph

from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship


class Path:
    """
    Container class for providing correct typing for resolved path classes
    from the driver. Does not provide any further functionality.
    """

    _nodes: Tuple[Node, ...]
    _relationships: Tuple[Relationship, ...]

    def __init__(self, nodes: Tuple[Node, ...], relationships: Tuple[Relationship, ...]):
        self._nodes = nodes
        self._relationships = relationships

    @property
    def graph(self) -> neo4j.graph.Graph:
        # We should only ever have already inflated models in this stage, so we can
        # assume that the `graph` property is defined
        return cast(neo4j.graph.Graph, self._nodes[0].graph)

    @property
    def nodes(self) -> Tuple[Node, ...]:
        return self._nodes

    @property
    def start_node(self) -> Node:
        return self._nodes[0]

    @property
    def end_node(self) -> Node:
        return self._nodes[-1]

    @property
    def relationships(self) -> tuple[Relationship, ...]:
        return self._relationships
