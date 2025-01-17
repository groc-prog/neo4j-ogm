from abc import abstractmethod
from copy import deepcopy
from typing import Any, Dict, List, Set, cast

from pydantic import BaseModel, PrivateAttr

from pyneo4j_ogm.options.model_options import ModelConfigurationValidator
from pyneo4j_ogm.registry import Registry


class ModelBase(BaseModel):
    """
    Base class for all models types. Implements configuration merging and deflation/inflation
    of model properties.
    """

    _registry: Registry = PrivateAttr()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry = Registry()

        parent_config = ModelConfigurationValidator(
            **getattr(
                super(cls, cls),
                "ogm_config",
                {},
            )
        )
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))

        merged_config = cls.__merge_config(parent_config.model_dump(), model_config.model_dump())
        setattr(cls, "ogm_config", ModelConfigurationValidator(**merged_config).model_dump())

    @classmethod
    @abstractmethod
    def pyneo4j_config(cls):
        """
        Returns the validated OGM model configuration.

        Returns:
            The validated configuration for either the node or relationship model.
        """
        pass  # pragma: no cover

    @classmethod
    def __merge_config(cls, current_config: Dict[str, Any], updated_config: Dict[str, Any]) -> Dict[str, Any]:
        merged_config = deepcopy(current_config)

        for key, value in updated_config.items():
            if isinstance(value, dict):
                # We are handling pre/post hooks, so we merge all the hooks defined
                merged_config[key] = cls.__merge_config(merged_config[key], value)
            elif isinstance(value, list):
                # We deal with sets of hook functions
                if key not in merged_config:
                    merged_config[key] = []

                cast(List, merged_config[key]).extend(value)
            elif isinstance(value, set):
                # We are dealing with labels, so merge them to allow for label inheritance
                merged_config[key] = cast(Set, merged_config[key]).union(value)
            else:
                merged_config[key] = value

        return merged_config
