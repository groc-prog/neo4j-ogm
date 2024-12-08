import re
from copy import copy
from typing import ClassVar, Set, cast

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.options.model_options import NodeConfig


class NodeModel(ModelBase):
    """
    Base model for all node models. Every node model should inherit from this class to define a
    model.
    """

    ogm_config: ClassVar[NodeConfig]

    def __init_subclass__(cls, **kwargs):
        # We get the labels defined in the config here since the config might get merged with
        # some other values when calling super().__init_subclass__()
        defined_labels = set() if "labels" not in cls.ogm_config else copy(cls.ogm_config["labels"])
        super().__init_subclass__(**kwargs)

        # If no labels are explicitly defined, we generate some from the class name
        if len(defined_labels) == 0:
            if "labels" not in cls.ogm_config:
                raise ValueError("labels property not initialized")

            defined_labels = re.findall(r"[A-Z][a-z]*|[A-Z]+(?=[A-Z][a-z]|$)", cls.__name__)
            cast(Set[str], cls.ogm_config["labels"]).update(defined_labels)
