from typing import Any, Dict, List, Tuple, Type, Union

from pydantic import VERSION, BaseModel

from pyneo4j_ogm.options.field_options import (
    DataTypeConstraint,
    EntityIndex,
    ExistenceConstraint,
    FullTextIndex,
    PointIndex,
    PropertyIndex,
    RangeIndex,
    TextIndex,
    UniquenessConstraint,
    VectorIndex,
)

IS_PYDANTIC_V2 = int(VERSION.split(".", 1)[0]) >= 2


def get_field_options(
    field,
) -> Tuple[
    List[Union[UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex]],
    List[Union[UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, EntityIndex, PropertyIndex, PointIndex]],
]:
    """
    Returns the defined options from a model field.

    Args:
        field (Union[ModelField, FieldInfo]): The field to get the options for. In
            Pydantic v1, this will be a `ModelField`, in v2 this will be `FieldInfo`

    Returns:
        Tuple[Optional[Neo4jOptions], Optional[MemgraphOptions]]: A tuple containing the first  encountered
            `Neo4jOptions and MemgraphOptions` instances or `None` if no options have been defined.
    """
    if IS_PYDANTIC_V2:
        neo4j_options = [
            annotated
            for annotated in field.metadata
            if isinstance(
                annotated, (UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex)
            )
        ]
        memgraph_options = [
            annotated
            for annotated in field.metadata
            if isinstance(
                annotated,
                (UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, EntityIndex, PropertyIndex, PointIndex),
            )
        ]
    else:
        metadata = getattr(field.outer_type_, "__metadata__", [])
        neo4j_options = [
            annotated
            for annotated in metadata
            if isinstance(
                annotated, (UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex)
            )
        ]
        memgraph_options = [
            annotated
            for annotated in metadata
            if isinstance(
                annotated,
                (UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, EntityIndex, PropertyIndex, PointIndex),
            )
        ]

    return (
        neo4j_options,
        memgraph_options,
    )


def get_model_fields(model: Type[BaseModel]) -> Dict[str, Any]:
    """
    Returns a dictionary containing all model fields for a given Pydantic model.

    Args:
        model (Type[BaseModel]): The model to get the fields for.

    Returns:
        Union[Dict[str, ModelField], Dict[str, FieldInfo]]: The field dictionary.
    """
    if IS_PYDANTIC_V2:
        return model.model_fields

    return model.__fields__


def get_model_dump(model: BaseModel, *args, **kwargs):
    """
    Returns the model as a dictionary using Pydantic's `model_dump` or `dict` methods.
    This method also accepts all arguments the Pydantic method accepts.

    Args:
        model (BaseModel): The model to dump.

    Returns:
        Dict[Any, Any]: The dumped model.
    """
    if IS_PYDANTIC_V2:
        return model.model_dump(*args, **kwargs)
    else:
        return model.dict(*args, **kwargs)
