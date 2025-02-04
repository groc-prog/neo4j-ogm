from typing import Tuple, cast

from neo4j.graph import Graph

from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.models.relationship import RelationshipModel


class PathContainer:
    """
    Container class for providing correct typing for resolved path classes
    from the driver. Does not provide any further functionality.
    """

    _nodes: Tuple[NodeModel, ...]
    _relationships: Tuple[RelationshipModel, ...]

    def __init__(self, nodes: Tuple[NodeModel, ...], relationships: Tuple[RelationshipModel, ...]):
        self._nodes = nodes
        self._relationships = relationships

    @property
    def graph(self) -> Graph:
        # We should only ever have already inflated models in this stage, so we can
        # assume that the `graph` property is defined
        return cast(Graph, self._nodes[0].graph)

    @property
    def nodes(self) -> Tuple[NodeModel, ...]:
        return self._nodes

    @property
    def start_node(self) -> NodeModel:
        return self._nodes[0]

    @property
    def end_node(self) -> NodeModel:
        return self._nodes[-1]

    @property
    def relationships(self) -> tuple[RelationshipModel, ...]:
        return self._relationships
