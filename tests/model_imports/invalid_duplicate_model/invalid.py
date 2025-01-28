from pyneo4j_ogm.models.node import NodeModel


class NotUniqueNodeModelOne(NodeModel):
    ogm_config = {"labels": ["NotUnique"]}


class NotUniqueNodeModelTwo(NodeModel):
    ogm_config = {"labels": ["NotUnique"]}
