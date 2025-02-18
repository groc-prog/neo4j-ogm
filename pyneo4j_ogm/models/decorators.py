from asyncio import iscoroutinefunction
from functools import wraps
from typing import Dict, List, cast

from pyneo4j_ogm.exceptions import EntityDestroyedError, EntityNotHydratedError
from pyneo4j_ogm.logger import logger
from pyneo4j_ogm.types.model import ActionContext, ActionFunction, ActionType


def ensure_not_destroyed(wrapped_func):
    """
    Ensures that the instance has not been destroyed before the decorated method is called.

    Raises:
        EntityDestroyedError: If the instance has been marked as destroyed.

    Returns:
        Callable: A wrapped function that includes additional functionality for async functions.
    """

    @wraps(wrapped_func)
    async def wrapper(self, *args, **kwargs):
        if getattr(self, "_destroyed", False):
            logger.error("Graph entity %s has already been destroyed", self.__class__.__name__)
            raise EntityDestroyedError()

        return await wrapped_func(*args, **kwargs)

    return wrapper


def ensure_hydrated(wrapped_func):
    """
    Ensures that the instance has been hydrated before the decorated method is called.

    Raises:
        EntityNotHydratedError: If the instance has not been hydrated yet.

    Returns:
        Callable: A wrapped function that includes additional functionality for async functions.
    """

    @wraps(wrapped_func)
    async def wrapper(self, *args, **kwargs):
        if not getattr(self, "hydrated", False):
            logger.error("Graph entity %s has not been hydrated yet", self.__class__.__name__)
            raise EntityNotHydratedError()

        return await wrapped_func(*args, **kwargs)

    return wrapper


def wrap_with_actions(action: ActionType):
    def decorator(wrapped_func):
        @wraps(wrapped_func)
        async def wrapper(self, *args, **kwargs):
            ctx: ActionContext = {"type_": action}

            pre_actions = cast(Dict[ActionType, List[ActionFunction]], self._ogm_config.pre_actions)
            post_actions = cast(Dict[ActionType, List[ActionFunction]], self._ogm_config.post_actions)

            for action_func in pre_actions[action]:
                if iscoroutinefunction(action_func):
                    await action_func(ctx, *args, **kwargs)
                else:
                    action_func(ctx, *args, **kwargs)

            result = await wrapped_func(self, *args, **kwargs)

            for action_func in post_actions[action]:
                if iscoroutinefunction(action_func):
                    await action_func(ctx, result, *args, **kwargs)
                else:
                    action_func(ctx, result, *args, **kwargs)

            return result

        return wrapper

    return decorator
