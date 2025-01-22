# pylint: disable=line-too-long


class Pyneo4jError(Exception):
    """
    Base exception for all Pyneo4j exceptions.
    """


class NoClientFoundError(Pyneo4jError):
    """
    No active client is available for the query to use.
    """

    def __init__(self, *args) -> None:
        super().__init__("No active client found", *args)


class ClientNotInitializedError(Pyneo4jError):
    """
    Raised if a client method is called without initializing the client first.
    """

    def __init__(self, *args) -> None:
        super().__init__("Client not initialized", *args)


class ModelResolveError(Pyneo4jError):
    """
    The client failed to resolve a node/relationship to the corresponding model.
    """

    def __init__(self, *args) -> None:
        super().__init__(
            "Model could not be resolved. This might mean that your data fails the validation of the model.", *args
        )


class TransactionInProgressError(Pyneo4jError):
    """
    The client already has a open session/transaction in progress.
    """

    def __init__(self, *args) -> None:
        super().__init__("There is already a session/transaction in progress.", *args)


class NoTransactionInProgressError(Pyneo4jError):
    """
    There is no session/transaction to commit or roll back.
    """

    def __init__(self, *args) -> None:
        super().__init__("There is no active session/transaction to commit or roll back", *args)


class UnsupportedDatabaseVersionError(Pyneo4jError):
    """
    The version of the connected database is not supported.
    """

    def __init__(self, *args) -> None:
        super().__init__("The version of the connected database is not supported", *args)


class InflationError(Pyneo4jError):
    """
    The graph entity could not be inflated into a model.
    """

    def __init__(self, model: str, *args):
        super().__init__(
            f"""The graph entity could not be inflated into the model {model}. This usually
            indicates that you are trying to inflate a model with nested properties for a
            Neo4j client while `allow_nested_properties` is disabled or that the stringified
            JSON is malformed and can not be recovered.""",
            *args,
        )


class DeflationError(Pyneo4jError):
    """
    A model instance could not be deflated into a storable format.
    """

    def __init__(self, model: str, *args):
        super().__init__(
            f"The model {model} could not be deflated into a storable format. This usually means that you are trying to store a data type which is not supported by the driver.",
            *args,
        )
