from typing import Any, Callable, Dict, List, Set, Tuple, Union

from pydantic import BaseModel, Field
from typing_extensions import TypedDict

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

    labels: Union[str, List[str], Set[str]]


class RelationshipConfig(BaseConfig, total=False):
    """
    Configuration options for relationship models used to control behavior and trigger side effects
    """

    type: str


class ModelConfigurationValidator(BaseModel):
    """
    Validation model for Node/Relationship model options
    """

    pre_hooks: Dict[Hooks, List[Callable]] = Field({})
    post_hooks: Dict[Hooks, List[Callable]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    labels: List[str] = Field([])
    type: str = Field("")

    if IS_PYDANTIC_V2:

        @field_validator("pre_hooks", "post_hooks", mode="before")
        @classmethod
        def normalize_hooks_pydantic_v2(cls, value: Any):
            return cls.__normalize_hooks(value)

        @field_validator("labels", mode="before")
        @classmethod
        def normalize_labels_pydantic_v2(cls, value: Any):
            return cls.__normalize_labels(value)

    else:

        @validator("pre_hooks", "post_hooks", pre=True)
        def normalize_hooks_pydantic_v1(cls, value: Any):
            return cls.__normalize_hooks(value)

        @validator("labels", pre=True)
        def normalize_labels_pydantic_v1(cls, value: Any):
            return cls.__normalize_labels(value)

    @classmethod
    def __normalize_hooks(cls, value: Any) -> Dict[str, List[Callable]]:
        if not isinstance(value, dict):
            raise ValueError("Hooks must be a dictionary")

        normalized = {}
        for key, hooks in value.items():
            if isinstance(hooks, list):
                normalized[key] = [hook for hook in hooks if callable(hook)]
            elif callable(hooks):
                normalized[key] = [hooks]

        return normalized

    @classmethod
    def __normalize_labels(cls, value: Any) -> List[str]:
        if not isinstance(value, (str, list, set)):
            raise ValueError("Labels must be string|list|set")

        if isinstance(value, str):
            return [value]
        elif isinstance(value, set):
            return list(value)

        return value


class ValidatedNodeConfiguration(BaseModel):
    """
    Model for validated node configuration. Mainly used to prevent wrong type-checking.
    """

    pre_hooks: Dict[Hooks, List[Callable]] = Field({})
    post_hooks: Dict[Hooks, List[Callable]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    labels: List[str] = Field([])


class ValidatedRelationshipConfiguration(BaseModel):
    """
    Model for validated relationship configuration. Mainly used to prevent wrong type-checking.
    """

    pre_hooks: Dict[Hooks, List[Callable]] = Field({})
    post_hooks: Dict[Hooks, List[Callable]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    type: str = Field("")

    if IS_PYDANTIC_V2:

        @field_validator("type")
        @classmethod
        def normalize_type_pydantic_v2(cls, value: Any):
            return cls.__normalize_type(value)

    else:

        @validator("type")
        def normalize_type_pydantic_v1(cls, value: Any):
            return cls.__normalize_type(value)

    @classmethod
    def __normalize_type(cls, value: str) -> str:
        return value.upper()
