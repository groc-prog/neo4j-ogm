from typing import Literal, Union

Hooks = Union[
    Literal["create"],
    Literal["update"],
    Literal["delete"],
    Literal["refresh"],
    Literal["find_one"],
    Literal["find_many"],
    Literal["update_one"],
    Literal["update_many"],
    Literal["delete_one"],
    Literal["delete_many"],
    Literal["count"],
    Literal["start_node"],
    Literal["end_node"],
    str,
]
