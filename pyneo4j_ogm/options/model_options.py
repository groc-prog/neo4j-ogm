from typing import Any, Callable, Dict, List, Set, Tuple, TypedDict, Union

from pydantic import BaseModel

from pyneo4j_ogm.pydantic import IS_PYDANTIC_V2
from pyneo4j_ogm.types.graph import EagerFetchStrategy
from pyneo4j_ogm.types.model import Hooks

if IS_PYDANTIC_V2:
    from pydantic import field_validator
else:
    from pydantic import validator

HookConfiguration = Union[Callable, Tuple[bool, Callable]]


class BaseConfig(TypedDict, total=False):
    """
    Configuration options shared by all models.
    """

    pre_hooks: Dict[Hooks, Union[Callable, List[Callable]]]
    post_hooks: Dict[Hooks, Union[Callable, List[Callable]]]
    skip_constraint_creation: bool
    skip_index_creation: bool
    eager_fetch: bool
    eager_fetch_strategy: EagerFetchStrategy


class NodeConfig(BaseConfig, total=False):
    """
    Configuration options for node models used to control behavior and trigger side effects
    """

    labels: Union[str, List[str]]


class RelationshipConfig(BaseConfig, total=False):
    """
    Configuration options for relationship models used to control behavior and trigger side effects
    """

    type: str


class ModelConfigurationValidator(BaseModel):
    """
    Validation model for Node/Relationship model options
    """

    pre_hooks: Dict[Hooks, Union[Callable, List[Callable]]]
    post_hooks: Dict[Hooks, Union[Callable, List[Callable]]]
    skip_constraint_creation: bool
    skip_index_creation: bool
    eager_fetch: bool
    eager_fetch_strategy: EagerFetchStrategy
    labels: Set[str]
    type: str

    if IS_PYDANTIC_V2:

        @field_validator("pre_hooks", "post_hooks")
        @classmethod
        def normalize_hooks_pydantic_v2(cls, value: Any):
            if not isinstance(value, dict):
                raise ValueError("Hooks must be a dictionary")

            normalized = {}
            for key, hooks in value.items():
                if isinstance(hooks, list):
                    normalized[key] = [hook for hook in hooks if callable(hook)]
                elif callable(hooks):
                    normalized[key] = [hooks]

            return normalized

    else:

        @validator("pre_hooks", "post_hooks")
        @classmethod
        def normalize_hooks_pydantic_v1(cls, value: Any):
            if not isinstance(value, dict):
                raise ValueError("Hooks must be a dictionary")

            normalized = {}
            for key, hooks in value.items():
                if isinstance(hooks, list):
                    normalized[key] = [hook for hook in hooks if callable(hook)]
                elif callable(hooks):
                    normalized[key] = [hooks]

            return normalized
