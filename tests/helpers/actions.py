# pylint: disable=unused-argument


def get_sync_func():
    """
    Returns a sync mock function and a counter for tracking how many times the mock function
    has been called.

    Returns:
        A tuple where the first function returns the call count and the second is the mock
            function.
    """
    count = 0

    def sync_mock_func(*args, **kwargs):
        nonlocal count
        count = count + 1

    def get_count():
        return count

    return get_count, sync_mock_func


def get_async_func():
    """
    Returns a async mock function and a counter for tracking how many times the mock function
    has been called.

    Returns:
        A tuple where the first function returns the call count and the second is the mock
            function.
    """
    count = 0

    async def async_mock_func(*args, **kwargs):
        nonlocal count
        count = count + 1

    def get_count():
        return count

    return get_count, async_mock_func
