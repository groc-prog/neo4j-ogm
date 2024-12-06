from typing import Optional, Union

from pydantic import VERSION

from pyneo4j_ogm.fields.field_options import MemgraphOptions, Neo4jOptions

IS_PYDANTIC_V2 = int(VERSION.split(".", 1)[0]) >= 2


def get_field_options(field) -> Optional[Union[Neo4jOptions, MemgraphOptions]]:
    """
    Returns the defined options from a model field.

    Args:
        field (Union[ModelField, FieldInfo]): The field to get the options for. In
            Pydantic v1, this will be a `ModelField`, in v2 this will be `FieldInfo`

    Returns:
        Optional[Union[Neo4jOptions, MemgraphOptions]]: `Neo4jOptions or
            MemgraphOptions` instances or `None` if no options have been defined.
    """
    if IS_PYDANTIC_V2:
        metadata = [annotated for annotated in field.metadata if isinstance(annotated, (MemgraphOptions, Neo4jOptions))]
    else:
        metadata = getattr(field.outer_type_, "__metadata__", [])
        metadata = [annotated for annotated in metadata if isinstance(annotated, (MemgraphOptions, Neo4jOptions))]

    if len(metadata) == 0:
        return None

    return metadata[0]
