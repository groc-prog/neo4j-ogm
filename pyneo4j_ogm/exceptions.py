# pylint: disable=line-too-long


from typing import List, Optional, Union

from pyneo4j_ogm.types.graph import EntityType


class Pyneo4jOrmError(Exception):
    """
    Base exception for all Pyneo4j exceptions.
    """


class NoClientFoundError(Pyneo4jOrmError):
    """
    No active client is available for the query to use.
    """

    def __init__(self, *args) -> None:
        super().__init__(
            "No active client found. This could happen if you have not initialized any clients or have unregistered all active clients.",
            *args,
        )


class ClientNotInitializedError(Pyneo4jOrmError):
    """
    Raised if a client method is called without initializing the client first.
    """

    def __init__(self, *args) -> None:
        super().__init__("Client not connected to any database.", *args)


class ModelResolveError(Pyneo4jOrmError):
    """
    The client failed to resolve a node/relationship to the corresponding model.
    """

    def __init__(self, entity_labels_or_type: Optional[Union[List[str], str]] = None, *args) -> None:
        msg = (
            f"Graph entity {entity_labels_or_type} could not be resolved. This might mean that your data fails the validation of the model or the model was not registered."
            if entity_labels_or_type is not None
            else "Graph entity could not be resolved. This might mean that your data fails the validation of the model or the model was not registered."
        )
        super().__init__(msg, *args)


class NoTransactionInProgressError(Pyneo4jOrmError):
    """
    There is no session/transaction to commit or roll back.
    """

    def __init__(self, *args) -> None:
        super().__init__("There is no active session/transaction to commit or roll back.", *args)


class UnsupportedDatabaseVersionError(Pyneo4jOrmError):
    """
    The version of the connected database is not supported.
    """

    def __init__(self, min_version: str, *args) -> None:
        super().__init__(
            f"The version of the connected database is not supported. The version must be at least {min_version}", *args
        )


class InflationError(Pyneo4jOrmError):
    """
    The graph entity could not be inflated into a model.
    """

    def __init__(self, model: str, *args):
        super().__init__(
            f"The graph entity could not be inflated into the model {model}. This usually indicates that you are trying to inflate a model with nested properties for a Neo4j client while `allow_nested_properties` is disabled or that the stringified JSON is malformed and can not be recovered.",
            *args,
        )


class DeflationError(Pyneo4jOrmError):
    """
    A model instance could not be deflated into a storable format.
    """

    def __init__(self, model: str, *args):
        super().__init__(
            f"The model {model} could not be deflated into a storable format. This usually means that you are trying to store a data type which is not supported by the driver.",
            *args,
        )


class DuplicateModelError(Pyneo4jOrmError):
    """
    Two models use the same labels or type.
    """

    def __init__(self, modelOne: str, modelTwo: str, *args):
        super().__init__(
            f"The models {modelOne} and {modelTwo} share the same labels or type. For a client to be able to resolve models correctly, each model must have a unique set of labels or type.",
            *args,
        )


class MissingRelationshipPropertyTypes(Pyneo4jOrmError):
    """
    A RelationshipProperty is missing types for the node/relationship it is linked to.
    """

    def __init__(self, model: str, field: str, *args):
        super().__init__(
            f"Field {field} in model {model} is missing one or more generics. Both the node and relationship have to be defined when adding new relationship property fields to a model.",
            *args,
        )


class EntityDestroyedError(Pyneo4jOrmError):
    """
    A method interacting with the database has been called on a deleted entity.
    """

    def __init__(self, *args) -> None:
        super().__init__(
            "Destroyed model instances can not interact with the database. This usually means the model you are calling this method on has already been deleted.",
            *args,
        )


class EntityAlreadyCreatedError(Pyneo4jOrmError):
    """
    The instance has already been created in the database.
    """

    def __init__(self, model_name: str, element_id: str, *args) -> None:
        super().__init__(
            f"The {model_name} instance {element_id} has already been created in the database.",
            *args,
        )


class EntityNotHydratedError(Pyneo4jOrmError):
    """
    The instance has not been hydrated yet.
    """

    def __init__(self, *args) -> None:
        super().__init__(
            "The model instance has not been hydrated yet. Call the .create() method to create the entity in the database",
            *args,
        )


class EntityNotFoundError(Pyneo4jOrmError):
    """
    No entity matched by the ID or ElementId was found in the graph.
    """

    def __init__(self, entity_type: EntityType, labels_or_type: Union[List[str], str], id_: str, *args):
        entity_type_log = "node" if entity_type == EntityType.NODE else "relationship"
        labels_or_type_log = ":".join(labels_or_type) if isinstance(labels_or_type, list) else labels_or_type
        super().__init__(
            f"No {entity_type_log} of type {labels_or_type_log} was found for id {id_}. This usually means you have some concurrency issues or the graph has been updated by another query.",
            *args,
        )
