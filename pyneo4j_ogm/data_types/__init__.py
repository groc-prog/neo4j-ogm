from types import NoneType

from neo4j.spatial import CartesianPoint as _Neo4jCartesianPoint
from neo4j.spatial import WGS84Point as _Neo4jWGS84Point
from neo4j.time import Date as _Date
from neo4j.time import DateTime as _DateTime
from neo4j.time import Duration as _Duration
from neo4j.time import Time as _Time

from .spatial import CartesianPoint, WGS84Point
from .temporal import NativeDate, NativeDateTime, NativeDuration, NativeTime

ALLOWED_TYPES = [
    _Neo4jCartesianPoint,
    _Neo4jWGS84Point,
    _DateTime,
    _Date,
    _Time,
    _Duration,
    list,
    dict,
    bool,
    int,
    float,
    str,
    bytearray,
    tuple,
    NoneType,
]

ALLOWED_NEO4J_LIST_TYPES = [
    _Neo4jCartesianPoint,
    _Neo4jWGS84Point,
    _DateTime,
    _Date,
    _Time,
    _Duration,
    bool,
    int,
    float,
    str,
]

__all__ = [
    "ALLOWED_TYPES",
    "CartesianPoint",
    "WGS84Point",
    "NativeDateTime",
    "NativeDate",
    "NativeTime",
    "NativeDuration",
]
