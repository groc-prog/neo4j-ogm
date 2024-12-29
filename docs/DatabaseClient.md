## Database client

This is where the magic happens! The `Pyneo4jClient` is the main entry point for interacting with the database. It handles all the heavy lifting for you and your models. Because of this, we have to always have at least one client initialized before doing anything else.

### Connecting to the database

Before you can run any queries, you have to connect to a database. This is done by calling the `connect()` method of the `Pyneo4jClient` instance. The `connect()` method takes a few arguments:

- `uri`: The connection URI to the database.
- `skip_constraints`: Whether the client should skip creating any constraints defined on models when registering them. Defaults to `False`.
- `skip_indexes`: Whether the client should skip creating any indexes defined on models when registering them. Defaults to `False`.
- `*args`: Additional arguments that are passed directly to Neo4j's `AsyncDriver.driver()` method.
- `**kwargs`: Additional keyword arguments that are passed directly to Neo4j's `AsyncDriver.driver()` method.

```python
from pyneo4j_ogm import Pyneo4jClient

client = Pyneo4jClient()
await client.connect(uri="<connection-uri-to-database>", auth=("<username>", "<password>"), max_connection_pool_size=10, ...)

## Or chained right after the instantiation of the class
client = await Pyneo4jClient().connect(uri="<connection-uri-to-database>", auth=("<username>", "<password>"), max_connection_pool_size=10, ...)
```

After connecting the client, you will be able to run any cypher queries against the database. Should you try to run a query without connecting to a database first (it happens to the best of us), you will get a `NotConnectedToDatabase` exception.

### Closing an existing connection

Connections can explicitly be closed by calling the `close()` method. This will close the connection to the database and free up any resources used by the client. Remember to always close your connections when you are done with them!

```python
## Do some heavy-duty work...

## Finally done, so we close the connection to the database.
await client.close()
```

Once you closed the client, it will be seen as `disconnected` and if you try to run any further queries with it, you will get a `NotConnectedToDatabase` exception

### Registering models

Models are a core feature of pyneo4j-ogm, and therefore you probably want to use some. But to work with them, they have to be registered with the client by calling the `register_models()` method and passing in your models as a list:

```python
## Create a new client instance and connect ...

await client.register_models([Developer, Coffee, Consumed])
```

or by providing the path to a directory holding all your models. The `register_models_from_directory()` method will automatically discover all models in the directory and all of it's subdirectories and register them:

```python
## Create a new client instance and connect ...

await client.register_models_from_directory("path/to/models")
```

This is a crucial step, because if you don't register your models with the client, you won't be able to work with them in any way. Should you try to work with a model that has not been registered, you will get a `UnregisteredModel` exception. This exception also gets raised if a database model defines a relationship-property with other (unregistered) models as a target or relationship model and then runs a query with said relationship-property.

If you have defined any indexes or constraints on your models, they will be created automatically when registering them. You can prevent this behavior by passing `skip_constraints=True` or `skip_indexes=True` to the `connect()` method. If you do this, you will have to create the indexes and constraints yourself.

> **Note**: If you don't register your models with the client, you will still be able to run cypher queries directly with the client, but you will `lose automatic model resolution` from queries. This means that, instead of resolved models, the raw Neo4j query results are returned.

### Executing Cypher queries

Models aren't the only things capable of running queries. The client can also be used to run queries, with some additional functionality to make your life easier.

Node- and RelationshipModels provide many methods for commonly used cypher queries, but sometimes you might want to execute a custom cypher with more complex logic. For this purpose, the client instance provides a `cypher()` method that allows you to execute custom cypher queries. The `cypher()` method takes three arguments:

- `query`: The cypher query to execute.
- `parameters`: A dictionary containing the parameters to pass to the query.
- `resolve_models`: Whether the client should try to resolve the models from the query results. Defaults to `True`.

This method will always return a tuple containing a list of results and a list of variables returned by the query. Internally, the client uses the `.values()` method of the Neo4j driver to get the results of the query.

> **Note:** If no models have been registered with the client and resolve_models is set to True, the client will not raise any exceptions but rather return the raw query results.

Here is an example of how to execute a custom cypher query:

```python
results, meta = await client.cypher(
  query="CREATE (d:Developer {uid: '553ac2c9-7b2d-404e-8271-40426ae80de0', name: 'John', age: 25}) RETURN d.name as developer_name, d.age",
  parameters={"name": "John Doe"},
  resolve_models=False,  ## Explicitly disable model resolution
)

print(results)  ## [["John", 25]]
print(meta)  ## ["developer_name", "d.age"]
```

### Batching cypher queries

We provide an easy way to batch multiple database queries together, regardless of whether you are using the client directly or via a model method. To do this you can use the `batch()` method, which has to be called with a asynchronous context manager like in the following example:

```python
async with client.batch():
  ## All queries executed inside the context manager will be batched into a single transaction
  ## and executed once the context manager exits. If any of the queries fail, the whole transaction
  ## will be rolled back.
  await client.cypher(
    query="CREATE (d:Developer {uid: $uid, name: $name, age: $age})",
    parameters={"uid": "553ac2c9-7b2d-404e-8271-40426ae80de0", "name": "John Doe", "age": 25},
  )
  await client.cypher(
    query="CREATE (c:Coffee {flavour: $flavour, milk: $milk, sugar: $sugar})",
    parameters={"flavour": "Espresso", "milk": False, "sugar": False},
  )

  ## Model queries also can be batched together without any extra work!
  coffee = await Coffee(flavour="Americano", milk=False, sugar=False).create()
```

You can batch anything that runs a query, be that a model method, a custom query or a relationship-property method. If any of the queries fail, the whole transaction will be rolled back and an exception will be raised.

For batching in queries, that you want to execute concurrently with other `neo4j` transactions, use `client.concurrent_batch()` and pass it's return value into any model method, custom query or relationship-property method as arg `batch_manager`

```python
coroutines_count = 3
queries_by_batch = 3
coroutines = []

async def batch_query(n):
    async with client.concurrent_batch() as batch:
        for i in range(queries_by_batch):
            await client.cypher(
                query="CREATE (d:Developer {uid: $uid, name: $name, age: $age})",
                parameters={"uid": uuid.uuid4(), "name": f"Dev {n}{i}", "age": 25+i},
                batch_manager=batch
            )

for i in range(coroutines_count):
    coroutines.append(
        batch_query(i)
    )
await asyncio.gather(*coroutines)
query_results = await session.run("MATCH (n) RETURN n")
results = await query_results.values()
await query_results.consume()

print(len(results))  ## 9 (if the database was empty before)
```

Non-batch queries are concurrent by default.

### Using bookmarks (Enterprise Edition only)

If you are using the Enterprise Edition of Neo4j, you can use bookmarks to keep track of the last transaction that has been committed. The client provides a `last_bookmarks` property that allows you to get the bookmarks from the last session. These bookmarks can be used in combination with the `use_bookmarks()` method. Like the `batch()` method, the `use_bookmarks()` method has to be called with a context manager. All queries run inside the context manager will use the bookmarks passed to the `use_bookmarks()` method. Here is an example of how to use bookmarks:

```python
## Create a new node and get the bookmarks from the last session
await client.cypher("CREATE (d:Developer {name: 'John Doe', age: 25})")
bookmarks = client.last_bookmarks

## Create another node, but this time don't get the bookmark
## When we use the bookmarks from the last session, this node will not be visible
await client.cypher("CREATE (c:Coffee {flavour: 'Espresso', milk: False, sugar: False})")

with client.use_bookmarks(bookmarks=bookmarks):
  ## All queries executed inside the context manager will use the bookmarks
  ## passed to the `use_bookmarks()` method.

  ## Here we will only see the node created in the first query
  results, meta = await client.cypher("MATCH (n) RETURN n")

  ## Model queries also can be batched together without any extra work!
  ## This will return no results, since the coffee node was created after
  ## the bookmarks were taken.
  coffee = await Coffee.find_many()
  print(coffee)  ## []
```

### Manual indexing and constraints

Most of the time, the creation of indexes/constraints will be handled by the models themselves. But it can still be handy to have a simple way of creating new ones. This is where the `create_lookup_index()`, `create_range_index`, `create_text_index`, `create_point_index` and `create_uniqueness_constraint()` methods come in.

First, let's take a look at how to create a custom index in the database. The `create_range_index`, `create_text_index` and `create_point_index` methods take a few arguments:

- `name`: The name of the index to create (Make sure this is unique!).
- `entity_type`: The entity type the index is created for. Can be either **EntityType.NODE** or **EntityType.RELATIONSHIP**.
- `properties`: A list of properties to create the index for.
- `labels_or_type`: The node labels or relationship type the index is created for.

The `create_lookup_index()` takes the same arguments, except for the `labels_or_type` and `properties` arguments.

The `create_uniqueness_constraint()` method also takes similar arguments.

- `name`: The name of the constraint to create.
- `entity_type`: The entity type the constraint is created for. Can be either **EntityType.NODE** or **EntityType.RELATIONSHIP**.
- `properties`: A list of properties to create the constraint for.
- `labels_or_type`: The node labels or relationship type the constraint is created for.

Here is an example of how to use the methods:

```python
## Creates a `RANGE` index for a `Coffee's` `sugar` and `flavour` properties
await client.create_range_index("hot_beverage_index", EntityType.NODE, ["sugar", "flavour"], ["Beverage", "Hot"])

## Creates a UNIQUENESS constraint for a `Developer's` `uid` property
await client.create_uniqueness_constraint("developer_constraint", EntityType.NODE, ["uid"], ["Developer"])
```

### Client utilities

The client also provides some additional utility methods, which mostly exist for convenience when writing tests or setting up environments:

- `is_connected()`: Returns whether the client is currently connected to a database.
- `drop_nodes()`: Drops all nodes from the database.
- `drop_constraints()`: Drops all constraints from the database.
- `drop_indexes()`: Drops all indexes from the database.
