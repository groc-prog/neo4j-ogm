from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.models.relationship import RelationshipModel


class NestedNodeModel(NodeModel):
    ogm_config = {"labels": ["Nested", "Duplicate"]}
