from typing import ClassVar, cast

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    RelationshipConfig,
)


class RelationshipModel(ModelBase):
    """
    Base model for all relationship models. Every relationship model should inherit from this class to have needed base
    functionality like de-/inflation and validation.
    """

    ogm_config: ClassVar[RelationshipConfig]

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(RelationshipConfig, getattr(super(cls, cls), "ogm_config"))

        # It does not make sense to merge relationship types when inheriting something so
        # we just always use the name if not stated otherwise
        if ("type" in parent_config and parent_config["type"] == model_config.type) or len(model_config.type) == 0:
            cls.ogm_config["type"] = cls.__name__.upper()
