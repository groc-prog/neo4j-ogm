from neo4j.spatial import CartesianPoint, WGS84Point
from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema


class CartesianPointAnnotation:
    """
    Pydantic-compatible implementation of the Neo4j native `CartesianPoint` class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:

        from_list_schema = core_schema.chain_schema(
            [
                core_schema.list_schema(items_schema=core_schema.float_schema()),
                core_schema.no_info_plain_validator_function(CartesianPoint),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(CartesianPoint),
                    core_schema.chain_schema(
                        [
                            core_schema.tuple_schema(items_schema=[core_schema.float_schema()]),
                            core_schema.no_info_plain_validator_function(CartesianPoint),
                        ]
                    ),
                    from_list_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                list, when_used="json", return_schema=core_schema.str_schema()
            ),
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
                core_schema.no_info_plain_validator_function(WGS84Point),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(WGS84Point),
                    core_schema.chain_schema(
                        [
                            core_schema.tuple_schema(items_schema=[core_schema.float_schema()]),
                            core_schema.no_info_plain_validator_function(WGS84Point),
                        ]
                    ),
                    from_list_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                list, when_used="json", return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.list_schema(items_schema=core_schema.float_schema()))
