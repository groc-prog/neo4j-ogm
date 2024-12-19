from typing import ClassVar, List, Set, cast

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    NodeConfig,
    ValidatedNodeConfiguration,
)
from pyneo4j_ogm.pydantic import parse_obj


class NodeModel(ModelBase):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.
    """

    ogm_config: ClassVar[NodeConfig]
    _ogm_config: ClassVar[ValidatedNodeConfiguration]

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(NodeConfig, getattr(super(cls, cls), "ogm_config"))
        parent_labels = cast(Set[str], parent_config["labels"]) if "labels" in parent_config else set()
        custom_labels = set(model_config.labels).difference(parent_labels)

        if "labels" not in cls.ogm_config:
            # We should never reach this line since the config should always be set in
            # the base class when initializing
            raise ValueError("'labels' property not initialized")  # pragma: no cover

        if len(custom_labels) == 0:
            cast(List[str], cls.ogm_config["labels"]).append(cls.__name__)
        else:
            for label in model_config.labels:
                if label not in cls.ogm_config["labels"]:
                    cast(List[str], cls.ogm_config["labels"]).append(label)

        cls._ogm_config = parse_obj(ValidatedNodeConfiguration, cls.ogm_config)
