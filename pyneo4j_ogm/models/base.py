from copy import deepcopy
from typing import Any, Dict

from pydantic import BaseModel, PrivateAttr

from pyneo4j_ogm.options.model_options import ModelConfigurationValidator
from pyneo4j_ogm.pydantic import get_model_dump
from pyneo4j_ogm.registry import Registry
from pyneo4j_ogm.types.graph import EagerFetchStrategy


class ModelBase(BaseModel):
    """
    Base class for all models types. Implements configuration merging and deflation/inflation
    of model properties.
    """

    _registry: Registry = PrivateAttr()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry = Registry()

        # Get parent config
        parent_config = getattr(
            super(cls, cls),
            "ogm_config",
            get_model_dump(
                ModelConfigurationValidator(
                    pre_hooks={},
                    post_hooks={},
                    skip_constraint_creation=False,
                    skip_index_creation=False,
                    eager_fetch=False,
                    eager_fetch_strategy=EagerFetchStrategy.DEFAULT,
                    labels=set(),
                    type="",
                )
            ),
        )

        merged_config = cls.__merge_config(parent_config, getattr(cls, "ogm_config", {}))
        setattr(cls, "ogm_config", get_model_dump(ModelConfigurationValidator(**merged_config)))

    @classmethod
    def __merge_config(cls, current_config: Dict[str, Any], updated_config: Dict[str, Any]) -> Dict[str, Any]:
        merged_config = deepcopy(current_config)

        for key, value in updated_config.items():
            if isinstance(value, dict):
                # We are handling pre/post hooks, so we merge all the hooks defined
                merged_config[key] = cls.__merge_config(merged_config[key], value)
            elif isinstance(value, list):
                # We either deal with sets of hook functions or labels
                # Regardless, we merge them together instead of replacing them
                if key not in merged_config:
                    merged_config[key] = []

                merged_config[key].append(*value)
            else:
                merged_config[key] = value

        return merged_config
