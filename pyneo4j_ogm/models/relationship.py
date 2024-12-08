from typing import ClassVar

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.options.model_options import RelationshipConfig


class RelationshipModel(ModelBase):
    """
    Base model for all relationship models. Every relationship model should inherit from this class to have needed base
    functionality like de-/inflation and validation.
    """

    ogm_config: ClassVar[RelationshipConfig]
