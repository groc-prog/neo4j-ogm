# 1.0.0 Roadmap

This release will focus on [`fixing existing issues`](https://github.com/groc-prog/pyneo4j-ogm/issues) and adding/enhancing existing features. Due to the nature of the planned changes listed below, there `will be breaking changes`.

## Note on open issues

There are a few open issues for a while now. Due to time limitations (and me being the only contributor to the project) there has been little to no progress on these. One of the fix which have to be done before the next release will be fixing those and any others to come. So if you find some bugs/have some requests, feel free to open a issue.

## It's finally time - Memgraph is coming

Some people who have already worked with Neo4j might have realized that there can be some shortcomings in performance if you work with a HUGE dataset (I'm talking millions of millions of entities). Of course yo can always optimize something, but most people (including me) would rather use something like [Memgraph](https://memgraph.com/) to get some immediate performance gains.

Until now some differences in the Cypher implementation between the two have prevented users of this package from switching to Memgraph, but I am happy to say that I will take the time to fully integrate Memgraph compatibility into this package. I don't know the exact route i will take with this, but it will be included, one way or another.

## Multithreading/Multiple clients

Until now, the process behind models and clients was implemented in a simple way:

1. You create a client
2. You create a model
3. You register a model with a client, resulting in the client being set on a property of the model

This approach worked fine for most use cases. But once you want to use multi-threading/multiple clients, things get a bit more complicated. So the next release will do a complete rewrite of this system, making it more versatile and robust for existing scenarios as well as once which can only be achieved with some dirty hacks.

## Query API

The query API and it's parameters where heavily inspired by the syntax of [`MongoDB`](https://www.mongodb.com/docs/manual/tutorial/query-documents/). I still think this approach is okay, but since there are some differences in Memgraph and Neo4j regarding queries, there will also be changes affecting the currently available operators.

Luckily, this also means it gives me an opportunity to expand the range of available operators to some more commonly used once.

## Cardinality

The current implementation of cardinality constraints was implemented more like a afterthought, which made it not as useful as it could be. This will also be improved and expanded

## Testing

The package was already pretty well tested, but from the next release on there will also be tests against different versions of Neo4j, Memgraph and Python. Until now there was just a generic constraint on the Neo4j version (which as i remember was randomly chosen since i could not be bothered to check it).

## Pydantic support

~~As it is the case now, Pydantic support for both 1.x and 2.x `will remain for the foreseeable future`~~.

Since Pydantic has reached EOL as defined [here](https://docs.pydantic.dev/2.7/version-policy/?utm_source=chatgpt.com#pydantic-v1), we will drop support for V1 in the next major release. This also makes things easier on our end, regarding refactoring and new features.

The migration guide for V1 to V2 can be found [here](https://docs.pydantic.dev/latest/migration/)

> **Note**: For those of you who are still using V1 of Pydantic - it's 2025 now and most users will not experience a lot of migration issues (especially when using Pydantic for basic data validation like in REST API's).

Pydantic does a lot of the validation/serialization work, and it might even help with automated migrations in the future (but we will see where this goes).

## Documentation and contributing

I know the documentation and contribution guidelines have been lacking, but it is my goal to get both of them up to date, so that:

1. You, the user of the package, actually knows how it works
2. People who want to contribute can do so without a million questions about how to get started

## Some more notes in this release

As mentioned in the start, there will be breaking changes. I don't plan to make this release backwards compatible, for the simple reason that it would probably be a nightmare.

Nevertheless i will of course do everything i can to keep the exposed API as similar as possible, to ease migrations for users of previous versions.
