# import asyncio
# import json
# from pydoc import resolve
# from typing import Any, Generic, Optional, Set, TypeVar

# from neo4j import AsyncGraphDatabase, GraphDatabase
# from neo4j.spatial import CartesianPoint, WGS84Point
# from neo4j.time import Date, DateTime, Duration, Time
# from pydantic import BaseModel

# from pyneo4j_ogm.clients.memgraph import MemgraphClient
# from pyneo4j_ogm.clients.neo4j import Neo4jClient
# from pyneo4j_ogm.data_types.temporal import NativeDate
# from pyneo4j_ogm.exceptions import DeflationError
# from pyneo4j_ogm.models.node import Node
# from pyneo4j_ogm.models.relationship import Relationship
# from tests.fixtures.db import Authentication, ConnectionString


# class Person(Node):
#     age: int
#     name: str
#     is_happy: bool


# class Related(Relationship):
#     days_since: int
#     close_friend: bool


# async def main():
#     class Node(Node):
#         pass

#     neo4j_client = Neo4jClient()
#     await neo4j_client.connect(ConnectionString.NEO4J.value, auth=Authentication.NEO4J.value)
#     await neo4j_client.register_models(Node, Person, Related)
#     # memgraph_client = await MemgraphClient().connect(
#     #     ConnectionString.MEMGRAPH.value, auth=Authentication.MEMGRAPH.value
#     # )

#     # await neo4j_client.drop_constraints()
#     # await neo4j_client.drop_indexes()
#     # await neo4j_client.drop_nodes()
#     # await memgraph_client.drop_constraints()
#     # await memgraph_client.drop_indexes()
#     # await memgraph_client.drop_nodes()

#     # await neo4j_client.cypher("CREATE (n:Node {dur: $dur})", {"dur": [1, "a", True]})
#     # await memgraph_client.cypher("CREATE (n:Node {dur: $dur})", {"dur": [1, "a", True]})

#     # await neo4j_client.cypher("CREATE (n:Node)")
#     # await neo4j_client.cypher("CREATE (n:Node)")
#     res = await neo4j_client.cypher("MATCH (n)-[r]->(m) RETURN [r, {nested: r}]")

#     pass


# asyncio.run(main())

from pydantic import BaseModel

from pyneo4j_ogm.data_types.relationship_property import RelationshipProperty
from pyneo4j_ogm.models.node import Node
from pyneo4j_ogm.models.relationship import Relationship

# class Person(Node):
#     pass


# class Relation(Relationship):
#     pass


# class Model(Node):
#     edge: RelationshipProperty[Person, Relation] = RelationshipProperty()
#     foo: str = ""
#     bar: str = ""


# model = Model()
# dump = model.model_dump()
# json_schema = model.model_json_schema()

# pass


class Model(BaseModel):
    id: str = "id"
    foo: int = 1


model = Model()
ser = model.model_dump(include={"foo"})

pass
