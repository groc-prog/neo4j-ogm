from enum import Enum


class EntityType(Enum):
    """
    Available entity types for both Neo4j and Memgraph. Even though relationships are
    called edges in Memgraph, this package will call them relationships anyways.
    """

    NODE = "NODE"
    RELATIONSHIP = "RELATIONSHIP"


class RelationshipDirection(Enum):
    """
    Available direction when defining a relationship.
    """

    INCOMING = 0
    OUTGOING = 1
    BOTH = 2


class OptionStrategy(Enum):
    """
    Available strategies used when creating indexes/constraints for multi-label nodes.
    """

    ALL = 0
    FIRST = 1
    LAST = 2
    SELECTED = 3
