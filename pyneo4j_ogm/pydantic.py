from typing import Any, Dict, Optional, Tuple, Type

from pydantic import VERSION, BaseModel

from pyneo4j_ogm.options.field_options import MemgraphOptions, Neo4jOptions

IS_PYDANTIC_V2 = int(VERSION.split(".", 1)[0]) >= 2


def get_field_options(field) -> Tuple[Optional[Neo4jOptions], Optional[MemgraphOptions]]:
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
        neo4j_options = [annotated for annotated in field.metadata if isinstance(annotated, Neo4jOptions)]
        memgraph_options = [annotated for annotated in field.metadata if isinstance(annotated, MemgraphOptions)]
    else:
        metadata = getattr(field.outer_type_, "__metadata__", [])
        neo4j_options = [annotated for annotated in metadata if isinstance(annotated, Neo4jOptions)]
        memgraph_options = [annotated for annotated in metadata if isinstance(annotated, MemgraphOptions)]

    return (
        neo4j_options[0] if len(neo4j_options) > 0 else None,
        memgraph_options[0] if len(memgraph_options) > 0 else None,
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
