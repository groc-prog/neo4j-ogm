from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship


class NestedNodeModel(Node):
    ogm_config = {"labels": ["Nested", "Duplicate"]}
