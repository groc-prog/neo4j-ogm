from pyneo4j_ogm.models.node import Node


class NotUniqueNodeModelOne(Node):
    ogm_config = {"labels": ["NotUnique"]}


class NotUniqueNodeModelTwo(Node):
    ogm_config = {"labels": ["NotUnique"]}
