from enum import Enum


class EntityType(Enum):
    """
    Available entity types for both Neo4j and Memgraph. Even though relationships are
    called edges in Memgraph, this package will call the relationships anyways.
    """

    NODE = "NODE"
    RELATIONSHIP = "RELATIONSHIP"


class RelationshipDirection(Enum):
    """
    Available direction when defining a relationship.
    """

    INCOMING = "INCOMING"
    OUTGOING = "OUTGOING"
    BOTH = "BOTH"
