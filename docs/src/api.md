# API Reference

Auto-generated from docstrings. Every exported symbol is listed below.

## Connection

```@docs
SurrealDB.connect
SurrealDB.close!
SurrealDB.status
SurrealDB.events
SurrealDB.SurrealClient
SurrealDB.AbstractConnection
SurrealDB.RemoteConnection
SurrealDB.EmbeddedConnection
```

## Authentication

```@docs
SurrealDB.signin!
SurrealDB.signup!
SurrealDB.authenticate!
SurrealDB.invalidate!
SurrealDB.RootAuth
SurrealDB.NamespaceAuth
SurrealDB.ScopedAuth
SurrealDB.JwtAuth
```

## Database scope

```@docs
SurrealDB.use!
SurrealDB.info
SurrealDB.version
SurrealDB.health
```

## Query and CRUD

```@docs
SurrealDB.query
SurrealDB.query_table
SurrealDB.query_one
SurrealDB.create
SurrealDB.select
SurrealDB.update
SurrealDB.delete
SurrealDB.insert
SurrealDB.upsert
SurrealDB.merge
SurrealDB.relate
SurrealDB.insert_relation
SurrealDB.patch
SurrealDB.patch_add
SurrealDB.patch_remove
SurrealDB.patch_replace
SurrealDB.let!
SurrealDB.unset!
```

## Live queries

```@docs
SurrealDB.live
SurrealDB.kill!
SurrealDB.LiveSubscription
```

## Transactions and sessions

```@docs
SurrealDB.begin!
SurrealDB.commit!
SurrealDB.cancel!
SurrealDB.attach!
SurrealDB.detach!
SurrealDB.sessions
SurrealDB.SurrealSession
```

## Import / Export

```@docs
SurrealDB.export_db
SurrealDB.import_db
```

## Embedded mode

```@docs
SurrealDB.libsurreal_load!
```

## Tables.jl and graph extensions

```@docs
SurrealDB.to_table
SurrealDB.to_metagraph
```

## Core types

```@docs
SurrealDB.RecordID
SurrealDB.Table
SurrealDB.SurrealValue
SurrealDB.Relationship
```

## Errors

```@docs
SurrealDB.SurrealDBError
SurrealDB.ServerError
SurrealDB.RPCError
SurrealDB.QueryError
SurrealDB.ValidationError
SurrealDB.ConfigurationError
SurrealDB.ThrownError
SurrealDB.SerializationError
SurrealDB.NotAllowedError
SurrealDB.NotFoundError
SurrealDB.AlreadyExistsError
SurrealDB.InternalError
SurrealDB.ConnectionError
SurrealDB.ConnectionUnavailableError
SurrealDB.UnsupportedEngineError
SurrealDB.UnsupportedFeatureError
SurrealDB.UnexpectedResponseError
SurrealDB.EmbeddedFFIError
```
