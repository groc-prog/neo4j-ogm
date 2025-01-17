from typing import List, Tuple, Union

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


def get_field_options(
    field,
) -> Tuple[
    List[Union[UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex]],
    List[Union[UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, PropertyIndex, PointIndex]],
]:
    """
    Returns the defined options from a model field.

    Args:
        field (Union[ModelField, FieldInfo]): The field to get the options for.

    Returns:
        Tuple[Optional[Neo4jOptions], Optional[MemgraphOptions]]: A tuple containing the first  encountered
            `Neo4jOptions and MemgraphOptions` instances or `None` if no options have been defined.
    """
    neo4j_options = [
        annotated
        for annotated in field.metadata
        if isinstance(annotated, (UniquenessConstraint, RangeIndex, TextIndex, PointIndex, VectorIndex, FullTextIndex))
    ]
    memgraph_options = [
        annotated
        for annotated in field.metadata
        if isinstance(
            annotated,
            (UniquenessConstraint, ExistenceConstraint, DataTypeConstraint, PropertyIndex, PointIndex),
        )
    ]

    return (
        neo4j_options,
        memgraph_options,
    )
