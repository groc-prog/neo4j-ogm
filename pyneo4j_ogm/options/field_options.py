from dataclasses import dataclass
from typing import List, Optional, Union

from pyneo4j_ogm.types.memgraph import MemgraphDataType


@dataclass
class UniquenessConstraint:
    """
    Used to define a uniqueness constraint for a given property. Can be used to create composite
    constraints by providing the `composite_key` property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. This does not have any
    effect when this index is used for a relationship.
    """

    composite_key: Optional[str] = None
    specified_label: Optional[str] = None


@dataclass
class ExistenceConstraint:
    """
    Memgraph specific constraint. Used to define a existence constraint for a given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. Can only be used with nodes.
    """

    specified_label: Optional[str] = None


@dataclass
class DataTypeConstraint:
    """
    Memgraph specific constraint. Used to define a data type constraint for a given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. Can only be used with nodes.
    """

    data_type: MemgraphDataType
    specified_label: Optional[str] = None


@dataclass
class PropertyIndex:
    """
    Memgraph specific constraint. Used to define a property index for a given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. `specified_label` can only be used with nodes.
    """

    specified_label: Optional[str] = None


@dataclass
class RangeIndex:
    """
    Neo4j specific constraint. Used to define a range index for the given property. Can be used to
    create composite indexes by providing the `composite_key` property.


    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. This does not have any
    effect when this index is used for a relationship.
    """

    composite_key: Optional[str] = None
    specified_label: Optional[str] = None


@dataclass
class TextIndex:
    """
    Neo4j specific constraint. Used to define a text index for the given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. This does not have any
    effect when this index is used for a relationship.
    """

    specified_label: Optional[str] = None


@dataclass
class PointIndex:
    """
    Used to define a point index for the given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. This does not have any
    effect when this index is used for a relationship.

    Can not be used on relationships when used on a model registered with a Memgraph client.
    """

    specified_label: Optional[str] = None


@dataclass
class FullTextIndex:
    """
    Neo4j specific constraint. Used to define a full-text index for the given property. Can be
    used to create composite indexes by providing the `composite_key` property.

    For multi-label nodes, specific labels can be defined with the `specified_labels` property.
    By default, the all available labels from the model will be used. This does not have any
    effect when this index is used for a relationship.
    """

    composite_key: Optional[str] = None
    specified_labels: Optional[Union[List[str], str]] = None


@dataclass
class VectorIndex:
    """
    Neo4j specific constraint. Used to define a vector index for the given property.

    For multi-label nodes, a specific label can be defined with the `specified_label` property.
    By default, the first available label from the model will be used. This does not have any
    effect when this index is used for a relationship.
    """

    specified_label: Optional[str] = None
