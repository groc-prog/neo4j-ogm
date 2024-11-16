"""
Exceptions raised by different methods.
"""


class Pyneo4jError(Exception):
    """
    Base exception for all Pyneo4j exceptions.
    """


class NoClientFoundError(Pyneo4jError):
    """
    No active client is available for the query to use.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("No active client found", *args)


class InvalidClientError(Pyneo4jError):
    """
    Invalid client was provided.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("Client must be a registered instance of `Pyneo4jClient`", *args)


class ClientNotInitializedError(Pyneo4jError):
    """
    Raised if a client method is called without initializing the client first.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("Client not initialized", *args)


class ModelResolveError(Pyneo4jError):
    """
    The client failed to resolve a node/relationship to the corresponding model.
    """

    def __init__(self, *args: object) -> None:
        super().__init__(
            "Model could not be resolved. This might mean that your data fails the validation of the model.", *args
        )


class TransactionInProgress(Pyneo4jError):
    """
    The client already has a open session/transaction in progress.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("There is already a session/transaction in progress.", *args)


class NoTransactionInProgress(Pyneo4jError):
    """
    There is no session/transaction to commit or roll back.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("There is no active session/transaction to commit or roll back", *args)


class UnsupportedDatabaseVersionError(Pyneo4jError):
    """
    The version of the connected database is not supported.
    """

    def __init__(self, *args: object) -> None:
        super().__init__("The version of the connected database is not supported", *args)
