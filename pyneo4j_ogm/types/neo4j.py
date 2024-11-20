from enum import Enum


class Neo4jIndexType(Enum):
    """
    Index types supported by Neo4j.
    """

    RANGE = "RANGE"
    TEXT = "TEXT"
    POINT = "POINT"
    TOKEN_LOOKUP = "TOKEN_LOOKUP"


class Neo4jConstraintType(Enum):
    """
    Constraint types supported by Neo4j.
    """

    UNIQUENESS = "UNIQUENESS"
