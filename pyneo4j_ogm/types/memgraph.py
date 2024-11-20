from enum import Enum
from typing import Mapping


class MemgraphIndexType(Enum):
    """
    Index types supported by Memgraph.
    """

    LABEL = "label"
    LABEL_AND_PROPERTY = "label+property"
    EDGE_TYPE = "edge-type"
    EDGE_TYPE_AND_PROPERTY = "edge-type+property"
    POINT = "point"


class MemgraphConstraintType(Enum):
    """
    Constraint types supported by Memgraph.
    """

    EXISTS = "exists"
    UNIQUE = "unique"
    DATA_TYPE = "data_type"


class MemgraphDataType(Enum):
    """
    Constraint data types supported by Memgraph.
    """

    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    LIST = "LIST"
    MAP = "MAP"
    DURATION = "DURATION"
    DATE = "DATE"
    LOCAL_TIME = "LOCALTIME"
    LOCAL_DATETIME = "LOCALDATETIME"
    ZONED_DATETIME = "ZONEDDATETIME"
    ENUM = "ENUM"
    POINT = "POINT"


MemgraphDataTypeMapping: Mapping[str, str] = {
    "LIST": MemgraphDataType.LIST.value,
    "MAP": MemgraphDataType.MAP.value,
    "DURATION": MemgraphDataType.DURATION.value,
    "DATE": MemgraphDataType.DATE.value,
    "INTEGER": MemgraphDataType.INTEGER.value,
    "FLOAT": MemgraphDataType.FLOAT.value,
    "STRING": MemgraphDataType.STRING.value,
    "BOOL": MemgraphDataType.BOOLEAN.value,
    "LOCAL TIME": MemgraphDataType.LOCAL_TIME.value,
    "LOCAL DATE TIME": MemgraphDataType.LOCAL_DATETIME.value,
    "ZONED DATE TIME": MemgraphDataType.ZONED_DATETIME.value,
    "ENUM": MemgraphDataType.ENUM.value,
    "POINT": MemgraphDataType.POINT.value,
}
