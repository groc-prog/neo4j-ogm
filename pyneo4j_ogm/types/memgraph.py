from enum import Enum


class MemgraphIndexType(Enum):
    LABEL = "label"
    LABEL_AND_PROPERTY = "label+property"
    EDGE_TYPE = "edge-type"
    EDGE_TYPE_AND_PROPERTY = "edge-type+property"
    POINT = "point"
