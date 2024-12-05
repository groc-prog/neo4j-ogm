from dataclasses import dataclass
from typing import Optional, Tuple, Union

from pyneo4j_ogm.types.graph import OptionStrategy
from pyneo4j_ogm.types.memgraph import MemgraphDataType

Option = Union[bool, Tuple[bool, OptionStrategy], Tuple[bool, OptionStrategy, str]]
MemgraphDataTypeOption = Union[
    Optional[MemgraphDataType], Tuple[MemgraphDataType, OptionStrategy], Tuple[MemgraphDataType, OptionStrategy, str]
]


@dataclass
class Neo4jOptions:
    """
    Used to define index/constraint options for a given property. This can be
    defined in 3 ways:

    1. With a `True/False` value indicating if the index/constraint will be applied
    2. With a `tuple` containing a `bool value` and the `OptionsStrategy`
    3. With a `tuple` containing a `bool value`, the `OptionsStrategy` and the
    affected labels
    """

    range_index: Option = False
    text_index: Option = False
    point_index: Option = False
    fulltext_index: Option = False
    vector_index: Option = False
    unique: Option = False


@dataclass
class MemgraphOptions:
    """
    Used to define index/constraint options for a given property. This can be
    defined in 3 ways:

    1. With a `True/False` value indicating if the index/constraint will be applied
    2. With a `tuple` containing a `bool value` and the `OptionsStrategy`
    3. With a `tuple` containing a `bool value`, the `OptionsStrategy` and the
    affected labels
    """

    index: Option = False
    property_index: Option = False
    point_index: Option = False
    unique: Option = False
    required: Option = False
    data_type: MemgraphDataTypeOption = None
