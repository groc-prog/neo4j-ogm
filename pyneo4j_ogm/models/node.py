from typing import ClassVar, List, Set, cast

from pydantic import PrivateAttr

from pyneo4j_ogm.models.base import ModelBase, generate_model_hash
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    NodeConfig,
    ValidatedNodeConfiguration,
)


class Node(ModelBase):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.
    """

    ogm_config: ClassVar[NodeConfig]
    """
    Configuration for the pyneo4j model, should be a dictionary conforming to [`NodeConfig`][pyneo4j_ogm.options.model_options.NodeConfig].
    """

    _ogm_config: ClassVar[ValidatedNodeConfiguration] = PrivateAttr()

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

        cls._ogm_config = ValidatedNodeConfiguration.model_validate(cls.ogm_config)
        cls._hash = generate_model_hash(cls._ogm_config.labels)
