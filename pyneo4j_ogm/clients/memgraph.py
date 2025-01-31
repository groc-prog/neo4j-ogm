import re
from typing import Dict, List, Optional, Self, Type, TypedDict, Union, cast
from uuid import uuid4

from neo4j.exceptions import ClientError

from pyneo4j_ogm.clients.base import Pyneo4jClient, ensure_initialized
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.models.relationship import RelationshipModel
from pyneo4j_ogm.options.field_options import (
    DataTypeConstraint,
    ExistenceConstraint,
    PointIndex,
    PropertyIndex,
    UniquenessConstraint,
)
from pyneo4j_ogm.pydantic import get_field_options
from pyneo4j_ogm.queries.query_builder import QueryBuilder
from pyneo4j_ogm.types.graph import EntityType
from pyneo4j_ogm.types.memgraph import (
    MemgraphConstraintType,
    MemgraphDataType,
    MemgraphDataTypeMapping,
    MemgraphIndexType,
)


class IndexConstraintMapping(TypedDict):
    labels_or_type: List[str]
    properties: List[str]
    has_labels_or_type_specified: bool
    data_type: Optional[MemgraphDataType]


ModelInitializationMapping = Dict[
    Type[Union[UniquenessConstraint, PointIndex, PropertyIndex, DataTypeConstraint, ExistenceConstraint]],
    Dict[str, IndexConstraintMapping],
]


class MemgraphClient(Pyneo4jClient):
    """
    Memgraph client used for interacting with a Memgraph database. Provides basic functionality for querying, indexing,
    constraints and other utilities.
    """

    async def drop_constraints(self) -> Self:
        logger.debug("Discovering constraints")
        constraints, _ = await self.cypher("SHOW CONSTRAINT INFO", auto_committing=True)

        if len(constraints) == 0:
            return self

        logger.warning("Dropping %d constraints", len(constraints))
        for constraint in constraints:
            match constraint[0]:
                case MemgraphConstraintType.EXISTS.value:
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT EXISTS (n.{constraint[2]})",
                        auto_committing=True,
                    )
                case MemgraphConstraintType.UNIQUE.value:
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT {', '.join([f'n.{constraint_property}' for constraint_property in constraint[2]])} IS UNIQUE",
                        auto_committing=True,
                    )
                case MemgraphConstraintType.DATA_TYPE.value:
                    # Some data types in Memgraph are returned differently that what is used when creating them
                    # Because of that we have to do some additional mapping when dropping them
                    await self.cypher(
                        f"DROP CONSTRAINT ON (n:{constraint[1]}) ASSERT n.{constraint[2]} IS TYPED {MemgraphDataTypeMapping[constraint[3]]}",
                        auto_committing=True,
                    )

        logger.info("%d constraints dropped", len(constraints))
        return self

    async def drop_indexes(self) -> Self:
        logger.debug("Discovering indexes")
        indexes, _ = await self.cypher("SHOW INDEX INFO", auto_committing=True)

        if len(indexes) == 0:
            return self

        logger.warning("Dropping %d indexes", len(indexes))
        for index in indexes:
            match index[0]:
                case MemgraphIndexType.EDGE_TYPE.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.EDGE_TYPE_AND_PROPERTY.value:
                    await self.cypher(f"DROP EDGE INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.LABEL.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}", auto_committing=True)
                case MemgraphIndexType.LABEL_AND_PROPERTY.value:
                    await self.cypher(f"DROP INDEX ON :{index[1]}({index[2]})", auto_committing=True)
                case MemgraphIndexType.POINT.value:
                    await self.cypher(f"DROP POINT INDEX ON :{index[1]}({index[2]})", auto_committing=True)

        logger.info("%d indexes dropped", len(indexes))
        return self

    @ensure_initialized
    async def existence_constraint(self, label: str, property_: str) -> Self:
        """
        Creates a new existence constraint for a node with a given label. Can only be used to create existence constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            property_ (str): The property which should be affected by the constraint.

        Returns:
            Self: The client.
        """
        logger.info("Creating existence constraint on %s", label)
        node_pattern = QueryBuilder.node_pattern("n", label)

        logger.debug("Creating existence constraint for %s on property %s", label, property_)
        await self.cypher(f"CREATE CONSTRAINT ON {node_pattern} ASSERT EXISTS (n.{property_})", auto_committing=True)

        return self

    @ensure_initialized
    async def uniqueness_constraint(self, label: str, properties: Union[List[str], str]) -> Self:
        """
        Creates a new uniqueness constraint for a node with a given label. Can only be used to create uniqueness constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            properties (Union[List[str], str]): The properties which should be affected by the constraint.

        Returns:
            Self: The client.
        """
        logger.info("Creating uniqueness constraint on %s", label)
        normalized_properties = [properties] if isinstance(properties, str) else properties

        node_pattern = QueryBuilder.node_pattern("n", label)
        property_pattern = ", ".join(f"n.{property_}" for property_ in normalized_properties)

        logger.debug("Creating uniqueness constraint for %s on properties %s", label, property_pattern)
        await self.cypher(
            f"CREATE CONSTRAINT ON {node_pattern} ASSERT {property_pattern} IS UNIQUE", auto_committing=True
        )

        return self

    @ensure_initialized
    async def data_type_constraint(self, label: str, property_: str, data_type: MemgraphDataType) -> Self:
        """
        Creates a new data type constraint for a node with a given label. Can only be used to create data type constraints
        on nodes.

        Args:
            label (str): The label on which the constraint will be created.
            property_ (str): The property which should be affected by the constraint.
            data_type (MemgraphDataType): The data type to enforce.

        Raises:
            ClientError: If a data type constraint already exists on the label-property pair.

        Returns:
            Self: The client.
        """
        logger.info("Creating data type constraint on %s for type %s", label, data_type.value)
        node_pattern = QueryBuilder.node_pattern("n", label)

        # Unlike other constraint/index types, the data type constraint throw a error if we try to create
        # another constraint for the same label/property combination
        try:
            logger.debug("Creating data type constraint for %s on property %s", label, property_)
            await self.cypher(
                f"CREATE CONSTRAINT ON {node_pattern} ASSERT n.{property_} IS TYPED {data_type.value}",
                auto_committing=True,
            )
        except Exception as exc:
            pattern = r"^Constraint IS TYPED \S+ on :\S+\(.*?\) already exists$"
            if not (
                isinstance(exc, ClientError)
                and exc.code == "Memgraph.ClientError.MemgraphError.MemgraphError"
                and exc.message is not None
                and re.match(pattern, exc.message)
            ):
                raise exc

        return self

    @ensure_initialized
    async def entity_index(self, label_or_edge: str, entity_type: EntityType) -> Self:
        """
        Creates a label/edge index.

        Args:
            label_or_edge (str): Label/edge in which the index is created.
            entity_type (EntityType): The type of graph entity for which the index will be created.

        Returns:
            Self: The client.
        """
        logger.info("Creating %s index for %s", "label" if entity_type == EntityType.NODE else "edge", label_or_edge)
        await self.cypher(
            f"CREATE {'EDGE ' if entity_type == EntityType.RELATIONSHIP else ''} INDEX ON :{label_or_edge}",
            auto_committing=True,
        )

        return self

    @ensure_initialized
    async def property_index(self, entity_type: EntityType, label_or_edge: str, property_: str) -> Self:
        """
        Creates a label/property or edge/property pair index.

        Args:
            entity_type (EntityType): The type of graph entity for which the index will be created.
            label_or_edge (str): Label/edge in which the index is created.
            property_ (str): The property which should be affected by the index.

        Returns:
            Self: The client.
        """
        logger.info(
            "Creating %s pair index for %s on %s",
            "label" if entity_type == EntityType.NODE else "edge",
            label_or_edge,
            property_,
        )
        await self.cypher(
            f"CREATE {'EDGE ' if entity_type == EntityType.RELATIONSHIP else ''} INDEX ON :{label_or_edge}({property_})",
            auto_committing=True,
        )

        return self

    @ensure_initialized
    async def point_index(self, label: str, property_: str) -> Self:
        """
        Creates a point index.

        Args:
            label (str): Label/edge in which the index is created.
            property_ (str): The property which should be affected by the index.

        Returns:
            Self: The client.
        """
        logger.info(
            "Creating point index for %s on %s",
            label,
            property_,
        )
        await self.cypher(f"CREATE POINT INDEX ON :{label}({property_})", auto_committing=True)

        return self

    @ensure_initialized
    async def _check_database_version(self) -> None:
        # I'm not sure if we actually need/can to check anything here since the server info
        # only states 'Neo4j/v5.11.0 compatible graph database server - Memgraph'
        pass

    @ensure_initialized
    async def _initialize_models(self) -> None:
        registered_hashes = set(self._registered_models.keys())

        for model_hash in registered_hashes.difference(self._initialized_model_hashes):
            model = self._registered_models[model_hash]
            entity_type = EntityType.NODE if issubclass(model, NodeModel) else EntityType.RELATIONSHIP

            if (model._ogm_config.skip_constraint_creation and model._ogm_config.skip_index_creation) or (
                self._skip_index_creation and self._skip_constraint_creation
            ):
                logger.debug("Constraint and index creation disabled for model %s, skipping", model.__name__)
                self._initialized_model_hashes.add(model_hash)
                continue

            logger.debug("Initializing model %s", model.__name__)
            # This mapping will hold all indexes and constraints we have to create
            mapping: ModelInitializationMapping = {}

            for field_name, field in model.model_fields.items():
                _, options = get_field_options(field)
                self.__generate_options_mapping(model, field_name, options, mapping)

            for index_or_constraint_type, mapped_options in mapping.items():
                for mapped_option in mapped_options.values():
                    if index_or_constraint_type == UniquenessConstraint:
                        await self.uniqueness_constraint(
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"],
                        )
                    elif index_or_constraint_type == ExistenceConstraint:
                        await self.existence_constraint(
                            mapped_option["labels_or_type"][0], mapped_option["properties"][0]
                        )
                    elif index_or_constraint_type == DataTypeConstraint:
                        await self.data_type_constraint(
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                            cast(MemgraphDataType, mapped_option["data_type"]),
                        )
                    elif index_or_constraint_type == PointIndex:
                        await self.point_index(
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                        )
                    elif index_or_constraint_type == PropertyIndex:
                        await self.property_index(
                            entity_type,
                            mapped_option["labels_or_type"][0],
                            mapped_option["properties"][0],
                        )

            self._initialized_model_hashes.add(model_hash)

    def __generate_options_mapping(
        self,
        model: Union[Type[NodeModel], Type[RelationshipModel]],
        field_name: str,
        options: List[Union[UniquenessConstraint, PointIndex, PropertyIndex, DataTypeConstraint, ExistenceConstraint]],
        mapping: ModelInitializationMapping,
    ) -> None:
        """
        Generates a mapping of options for a given model and field.

        This method processes a list of options, such as constraints and indexes, and updates
        the provided `mapping` object with configuration details. It supports both node and
        relationship models, handling properties like composite keys, data types, and labels.

        Args:
            model (Union[Type[NodeModel], Type[RelationshipModel]]): The model class to which the field belongs.
                Must be a subclass of either `NodeModel` or `RelationshipModel`.
            field_name (str): The name of the field to which the options apply.
            options (List[Union[UniquenessConstraint, PointIndex, PropertyIndex, DataTypeConstraint, ExistenceConstraint]]):
                A list of options specifying constraints or indexes to be applied to the field.
            mapping (ModelInitializationMapping): The mapping object that will be updated with the processed options.

        Raises:
            ValueError: If a specified label for an option is not valid for the given model.
            ValueError: If multiple conflicting data types are defined for a field with a `DataTypeConstraint`.
            ValueError: If conflicting labels or types are provided for a composite key.
        """
        is_node_model = issubclass(model, NodeModel)

        for option in options:
            has_composite_key = getattr(option, "composite_key", None) is not None
            map_key = getattr(option, "composite_key") if has_composite_key else str(uuid4())

            if type(option) not in mapping:
                mapping[type(option)] = {}

            mapped_options = mapping[type(option)]

            types_to_check = []
            if not (model._ogm_config.skip_constraint_creation or self._skip_constraint_creation):
                types_to_check.extend([UniquenessConstraint, ExistenceConstraint, DataTypeConstraint])
            if not (model._ogm_config.skip_index_creation or self._skip_index_creation):
                types_to_check.extend([PropertyIndex, PointIndex])

            if has_composite_key and map_key in mapped_options:
                mapped_options[map_key]["properties"].append(field_name)

            # FullTextIndex is the only index with different structure since it can create the same index
            # on multiple labels
            if isinstance(option, tuple(types_to_check)):
                specified_label: Optional[str] = None if not is_node_model else getattr(option, "specified_label", None)
                has_specified_label = specified_label is not None

                # Validate that the provided specified_label is actually one defined on the model
                if is_node_model and has_specified_label and specified_label not in model._ogm_config.labels:
                    raise ValueError(f"'{specified_label}' is not a valid label for model {model.__name__}")

                # Define defaults for each option we come across to make handling easier later on
                if map_key not in mapped_options:
                    mapped_options[map_key] = {
                        "labels_or_type": [model._ogm_config.labels[0] if is_node_model else model._ogm_config.type],
                        "properties": [field_name],
                        "has_labels_or_type_specified": False,
                        "data_type": None,
                    }

                # If it is a DataTypeConstraint, we need to set the data_type property and validate that there has
                # only been one or the same data type defined
                if isinstance(option, DataTypeConstraint):
                    if mapped_options[map_key]["data_type"] is None:
                        mapped_options[map_key]["data_type"] = option.data_type

                # If we are handling a composite key and the specified labels diverge, we throw a error since we
                # don't know what label to use
                if (
                    has_specified_label
                    and mapped_options[map_key]["has_labels_or_type_specified"]
                    and specified_label != mapped_options[map_key]["labels_or_type"][0]
                ):
                    raise ValueError(
                        f"Multiple different labels/types defined for composite key {map_key} in model {model.__name__}"
                    )

                # If no custom labels have been defined, we either use the existing label if we are currently handling
                # a composite key or we generate a random UUID
                if has_specified_label:
                    mapped_options[map_key]["labels_or_type"] = [specified_label]
                    mapped_options[map_key]["has_labels_or_type_specified"] = True
