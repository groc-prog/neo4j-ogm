from typing import Callable, Dict, List, Tuple, TypedDict, Union

from pyneo4j_ogm.types.graph import EagerFetchStrategy

HookConfiguration = Union[Callable, Tuple[bool, Callable]]


class BaseOptions(TypedDict, total=False):
    """
    Options shared by all models.
    """

    pre_hooks: Dict[str, Union[HookConfiguration, List[HookConfiguration]]]
    post_hooks: Dict[str, Union[HookConfiguration, List[HookConfiguration]]]
    skip_constraint_creation: bool
    skip_index_creation: bool
    eager_fetch: bool
    eager_fetch_strategy: EagerFetchStrategy


class NodeOptions(BaseOptions):
    """
    Options for node models used to control behavior and trigger side effects
    """

    labels: Union[str, List[str]]


class RelationshipOptions(BaseOptions):
    """
    Options for relationship models used to control behavior and trigger side effects
    """

    type: str
