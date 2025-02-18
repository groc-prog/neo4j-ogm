import hashlib
from typing import List, Union


def generate_model_hash(labels_or_type: Union[List[str], str]) -> str:
    """
    Returns a hash identifier for the given labels or type.

    Args:
        labels_or_type (Union[List[str], str]): The labels/type of the node/relationship.

    Returns:
        str: The generated hash.
    """
    combined = (
        f"__relationship_model_{labels_or_type}"
        if not isinstance(labels_or_type, list)
        else f"__node_model_{'__'.join(sorted(labels_or_type))}"
    )
    return hashlib.sha256(combined.encode()).hexdigest()
