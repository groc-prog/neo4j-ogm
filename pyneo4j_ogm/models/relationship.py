from typing import ClassVar, Generic, Optional, Self, TypeVar, cast

from neo4j.graph import Relationship
from pydantic import PrivateAttr

from pyneo4j_ogm.models.base import ModelBase
from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.options.model_options import (
    ModelConfigurationValidator,
    RelationshipConfig,
    ValidatedRelationshipConfiguration,
)

T = TypeVar("T", bound=NodeModel)
U = TypeVar("U", bound=NodeModel)


class RelationshipModel(ModelBase, Generic[T, U]):
    """
    Base model for all relationship models. Every relationship model should inherit from this class to have needed base
    functionality like de-/inflation and validation.
    """

    ogm_config: ClassVar[RelationshipConfig]
    """
    Configuration for the pyneo4j model, should be a dictionary conforming to [`RelationshipConfig`][pyneo4j_ogm.options.model_options.RelationshipConfig].
    """

    _ogm_config: ClassVar[ValidatedRelationshipConfiguration] = PrivateAttr()
    _start_node: Optional[T] = None
    _end_node: Optional[U] = None

    def __init_subclass__(cls, **kwargs):
        model_config = ModelConfigurationValidator(**getattr(cls, "ogm_config", {}))
        super().__init_subclass__(**kwargs)

        parent_config = cast(RelationshipConfig, getattr(super(cls, cls), "ogm_config"))

        # It does not make sense to merge relationship types when inheriting something so
        # we just always use the name if not stated otherwise
        if ("type" in parent_config and parent_config["type"] == model_config.type) or len(model_config.type) == 0:
            cls.ogm_config["type"] = cls.__name__.upper()

        cls._ogm_config = ValidatedRelationshipConfiguration.model_validate(cls.ogm_config)

    @property
    def start_node(self) -> Optional[T]:
        """
        Start node model for this relationship.
        """
        return self._start_node

    @property
    def end_node(self) -> Optional[U]:
        """
        End node model for this relationship.
        """
        return self._end_node

    @classmethod
    def _inflate(cls, graph_entity: Relationship, start_node: Optional[T] = None, end_node: Optional[U] = None) -> Self:
        """
        Inflates a graph entity from the Neo4j driver into the current model.

        Args:
            graph_entity (Relationship): The graph relationship to inflate.
            start_node (Optional[NodeModel]): The inflated node model. This should be the start node returned with
                the relationship from the DB. Defaults to `None`.
            end_node (Optional[NodeModel]): The inflated node model. This should be the end node returned with
                the relationship from the DB. Defaults to `None`.

        Returns:
            Self: A inflated instance of the current model.
        """
        inflated = super()._inflate(graph_entity)

        if start_node is not None:
            inflated._start_node = start_node
        if end_node is not None:
            inflated._end_node = end_node

        return inflated
