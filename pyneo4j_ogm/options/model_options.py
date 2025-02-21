from typing import Any, Dict, List, Set, Union

from pydantic import BaseModel, Field, field_validator
from typing_extensions import TypedDict

from pyneo4j_ogm.types.model import ActionFunction, ActionType, EagerFetchStrategy


class BaseConfig(TypedDict, total=False):
    """
    Configuration options shared by all models.
    """

    before_actions: Dict[ActionType, Union[ActionFunction, List[ActionFunction]]]
    after_actions: Dict[ActionType, Union[ActionFunction, List[ActionFunction]]]
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

    before_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    after_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    labels: List[str] = Field([])
    type: str = Field("")

    @field_validator("before_actions", "after_actions", mode="before")
    @classmethod
    def normalize_actions(cls, value: Any):
        if not isinstance(value, dict):
            raise ValueError("Actions must be defined as a dictionary")

        normalized = {}
        for key, actions in value.items():
            if isinstance(actions, list):
                normalized[key] = [action for action in actions if callable(action)]
            elif callable(actions):
                normalized[key] = [actions]

        return normalized

    @field_validator("labels", mode="before")
    @classmethod
    def normalize_labels(cls, value: Any):
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

    before_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    after_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    labels: List[str] = Field([])


class ValidatedRelationshipConfiguration(BaseModel):
    """
    Model for validated relationship configuration. Mainly used to prevent wrong type-checking.
    """

    before_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    after_actions: Dict[ActionType, List[ActionFunction]] = Field({})
    skip_constraint_creation: bool = Field(False)
    skip_index_creation: bool = Field(False)
    eager_fetch: bool = Field(False)
    eager_fetch_strategy: EagerFetchStrategy = Field(EagerFetchStrategy.COMBINED)
    type: str = Field("")

    @field_validator("type")
    @classmethod
    def normalize_type(cls, value: Any):
        return value.upper()
