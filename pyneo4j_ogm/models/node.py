import re
from typing import ClassVar, Set, cast

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.options.model_options import ModelConfigurationValidator, NodeConfig


class NodeModel(ModelBase):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.
    """

    ogm_config: ClassVar[NodeConfig]

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(NodeConfig, getattr(super(cls, cls), "ogm_config"))
        parent_labels = cast(Set[str], parent_config["labels"]) if "labels" in parent_config else set()
        custom_labels = model_config.labels.difference(parent_labels)

        if "labels" not in cls.ogm_config:
            # We should never reach this line since the config should always be set in
            # the base class when initializing
            raise ValueError("labels property not initialized")  # pragma: no cover

        if len(custom_labels) == 0:

            auto_labels = re.findall(r"[A-Z][a-z]*|[A-Z]+(?=[A-Z][a-z]|$)", cls.__name__)
            cast(Set[str], cls.ogm_config["labels"]).update(auto_labels)
        else:
            cls.ogm_config["labels"] = cast(Set[str], cls.ogm_config["labels"]).union(model_config.labels)
