import json
from typing import Any, Dict, List, Tuple, Type, TypeVar, Union

from pydantic import VERSION, BaseModel

from pyneo4j_ogm.options.field_options import (
    DataTypeConstraint,
    ExistenceConstraint,
    FullTextIndex,
    PointIndex,
    PropertyIndex,
    RangeIndex,
    TextIndex,
    UniquenessConstraint,
    VectorIndex,
)

T = TypeVar("T", bound=BaseModel)

IS_PYDANTIC_V2 = int(VERSION.split(".", 1)[0]) >= 2


def get_field_options(
    field,
) -> Tuple[
    List[Union[UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex]],
    List[Union[UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, PropertyIndex, PointIndex]],
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
                (UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, PropertyIndex, PointIndex),
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
                (UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, PropertyIndex, PointIndex),
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

    return model.dict(*args, **kwargs)


def get_model_dump_json(model: BaseModel, *args, **kwargs):
    """
    Returns the model as a JSON string using Pydantic's `model_dump_json` or `json` methods.
    This method also accepts all arguments the Pydantic method accepts.

    Args:
        model (BaseModel): The model to dump.

    Returns:
        Dict[Any, Any]: The dumped model.
    """
    if IS_PYDANTIC_V2:
        return model.model_dump_json(*args, **kwargs)

    return model.json(*args, **kwargs)


def get_model_schema(model: BaseModel, *args, **kwargs):
    """
    Returns the model's JSON schema using Pydantic's `model_json_schema` or `schema_json` methods.
    This method also accepts all arguments the Pydantic method accepts.

    Args:
        model (BaseModel): The model to dump.

    Returns:
        Dict[Any, Any]: The dumped model.
    """
    if IS_PYDANTIC_V2:
        return model.model_json_schema(*args, **kwargs)

    return json.loads(model.schema_json(*args, **kwargs))


def parse_obj(model: Type[T], obj: Any, *args, **kwargs) -> T:
    """
    Parses a model from a object. Compatible with both Pydantic V1 and V2.

    Args:
        model (Type[T]): The model to parse the object to.
        obj (Any): The object to parse.

    Raises:
        ValidationError: If the object could not be validated.

    Returns:
        T: The parsed model.
    """
    if IS_PYDANTIC_V2:
        return model.model_validate(obj, *args, **kwargs)

    return model.parse_obj(obj, *args, **kwargs)


def parse_json(model: Type[T], json: str, *args, **kwargs) -> T:
    """
    Parses a model from a JSON string. Compatible with both Pydantic V1 and V2.

    Args:
        model (Type[T]): The model to parse the JSON string to.
        obj (Any): The JSON string to parse.

    Raises:
        ValidationError: If the JSON string could not be validated.

    Returns:
        T: The parsed model.
    """
    if IS_PYDANTIC_V2:
        return model.model_validate_json(json, *args, **kwargs)

    return model.parse_raw(json, *args, **kwargs)
