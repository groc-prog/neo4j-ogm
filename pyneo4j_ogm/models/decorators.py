from functools import wraps

from pyneo4j_ogm.exceptions import EntityDestroyedError, EntityNotHydratedError
from pyneo4j_ogm.logger import logger


def ensure_not_destroyed(func):
    """
    Ensures that the instance has not been destroyed before the decorated method is called.

    Raises:
        EntityDestroyedError: If the instance has been marked as destroyed.

    Returns:
        Callable: A wrapped function that includes additional functionality for async functions.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if getattr(self, "_destroyed", False):
            logger.error("Graph entity %s has already been destroyed", self.__class__.__name__)
            raise EntityDestroyedError()

        return await func(*args, **kwargs)

    return wrapper


def ensure_hydrated(func):
    """
    Ensures that the instance has been hydrated before the decorated method is called.

    Raises:
        EntityNotHydratedError: If the instance has not been hydrated yet.

    Returns:
        Callable: A wrapped function that includes additional functionality for async functions.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not getattr(self, "hydrated", False):
            logger.error("Graph entity %s has not been hydrated yet", self.__class__.__name__)
            raise EntityNotHydratedError()

        return await func(*args, **kwargs)

    return wrapper
