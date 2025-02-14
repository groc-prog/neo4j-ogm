from types import NoneType

import neo4j.spatial
import neo4j.time

from .spatial import CartesianPoint, WGS84Point
from .temporal import NativeDate, NativeDateTime, NativeDuration, NativeTime

ALLOWED_TYPES = [
    neo4j.spatial.CartesianPoint,
    neo4j.spatial.WGS84Point,
    neo4j.time.DateTime,
    neo4j.time.Date,
    neo4j.time.Time,
    neo4j.time.Duration,
    list,
    dict,
    bool,
    int,
    float,
    str,
    bytearray,
    tuple,
]

ALLOWED_NEO4J_LIST_TYPES = [
    neo4j.spatial.CartesianPoint,
    neo4j.spatial.WGS84Point,
    neo4j.time.DateTime,
    neo4j.time.Date,
    neo4j.time.Time,
    neo4j.time.Duration,
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
