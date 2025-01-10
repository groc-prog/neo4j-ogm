from typing import Annotated

from neo4j.time import Date, DateTime, Duration, Time
from pydantic import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema


class NativeDateTimeAnnotation:
    """
    Pydantic-compatible annotation for the Neo4j native DateTime class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:

        from_iso_format_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(DateTime.from_iso_format),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=core_schema.union_schema([from_iso_format_schema]),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(DateTime),
                    core_schema.chain_schema(
                        [
                            core_schema.datetime_schema(),
                            core_schema.no_info_plain_validator_function(DateTime.from_native),
                        ]
                    ),
                    from_iso_format_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: instance.iso_format(), when_used="json", return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.datetime_schema())


class NativeDateAnnotation:
    """
    Pydantic-compatible annotation for the Neo4j native Date class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:
        from_iso_format_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(Date.from_iso_format),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=core_schema.union_schema([from_iso_format_schema]),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(Date),
                    core_schema.chain_schema(
                        [
                            core_schema.date_schema(),
                            core_schema.no_info_plain_validator_function(Date.from_native),
                        ]
                    ),
                    from_iso_format_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: instance.iso_format(), when_used="json", return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.date_schema())


class NativeTimeAnnotation:
    """
    Pydantic-compatible annotation for the Neo4j native Time class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:
        from_iso_format_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(Time.from_iso_format),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=core_schema.union_schema([from_iso_format_schema]),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(Time),
                    core_schema.chain_schema(
                        [
                            core_schema.time_schema(),
                            core_schema.no_info_plain_validator_function(Time.from_native),
                        ]
                    ),
                    from_iso_format_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: instance.iso_format(), when_used="json", return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.time_schema())


class NativeDurationAnnotation:
    """
    Pydantic-compatible annotation for the Neo4j native Duration class.
    """

    @classmethod
    def __get_pydantic_core_schema__(cls, *_) -> core_schema.CoreSchema:
        from_iso_format_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(Duration.from_iso_format),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=core_schema.union_schema([from_iso_format_schema]),
            python_schema=core_schema.union_schema(
                [
                    core_schema.is_instance_schema(Duration),
                    from_iso_format_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda instance: instance.iso_format(), when_used="json", return_schema=core_schema.str_schema()
            ),
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, _: core_schema.CoreSchema, handler: GetJsonSchemaHandler) -> JsonSchemaValue:
        return handler(core_schema.timedelta_schema())


NativeDateTime = Annotated[DateTime, NativeDateTimeAnnotation]
NativeDate = Annotated[Date, NativeDateAnnotation]
NativeTime = Annotated[Time, NativeTimeAnnotation]
NativeDuration = Annotated[Duration, NativeDurationAnnotation]
