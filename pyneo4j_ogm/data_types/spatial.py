from neo4j.spatial import CartesianPoint as Neo4jCartesianPoint
from neo4j.spatial import WGS84Point as Neo4jWGS84Point
from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from typing_extensions import Annotated


class CartesianPointAnnotation:
    """
    Pydantic-compatible implementation of the Neo4j native `CartesianPoint` class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:
        from_list_schema = core_schema.chain_schema(
            [
                core_schema.list_schema(items_schema=core_schema.float_schema()),
                core_schema.no_info_plain_validator_function(Neo4jCartesianPoint),
            ]
        )
        from_tuple_schema = core_schema.chain_schema(
            [
                core_schema.tuple_variable_schema(
                    core_schema.float_schema(),
                    min_length=1,
                ),
                core_schema.no_info_plain_validator_function(Neo4jCartesianPoint),
            ]
        )

        def serialize(
            val: Neo4jCartesianPoint,
            handler: core_schema.SerializerFunctionWrapHandler,
            info: core_schema.SerializationInfo,
        ):
            if info.mode_is_json():
                return handler(val)

            # FIXME: https://github.com/pydantic/pydantic/issues/11287
            return val

        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(Neo4jCartesianPoint),
                    from_list_schema,
                    from_tuple_schema,
                ]
            ),
            serialization=core_schema.wrap_serializer_function_ser_schema(serialize, info_arg=True),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.list_schema(items_schema=core_schema.float_schema()))


class WGS84PointAnnotation:
    """
    Pydantic-compatible implementation of the Neo4j native `WGS84Point` class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:
        from_list_schema = core_schema.chain_schema(
            [
                core_schema.list_schema(items_schema=core_schema.float_schema()),
                core_schema.no_info_plain_validator_function(Neo4jWGS84Point),
            ]
        )
        from_tuple_schema = core_schema.chain_schema(
            [
                core_schema.tuple_variable_schema(
                    core_schema.float_schema(),
                    min_length=1,
                ),
                core_schema.no_info_plain_validator_function(Neo4jWGS84Point),
            ]
        )

        def serialize(
            val: Neo4jWGS84Point,
            handler: core_schema.SerializerFunctionWrapHandler,
            info: core_schema.SerializationInfo,
        ):
            if info.mode_is_json():
                return handler(val)

            # FIXME: https://github.com/pydantic/pydantic/issues/11287
            return val

        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(Neo4jWGS84Point),
                    from_list_schema,
                    from_tuple_schema,
                ]
            ),
            serialization=core_schema.wrap_serializer_function_ser_schema(serialize, info_arg=True),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.list_schema(items_schema=core_schema.float_schema()))


CartesianPoint = Annotated[Neo4jCartesianPoint, CartesianPointAnnotation]
WGS84Point = Annotated[Neo4jWGS84Point, WGS84PointAnnotation]
