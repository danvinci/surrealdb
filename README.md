# SurrealDB.jl

[![Test](https://github.com/danvinci/surrealdb/actions/workflows/test.yml/badge.svg)](https://github.com/danvinci/surrealdb/actions/workflows/test.yml)
[![Benchmark](https://github.com/danvinci/surrealdb/actions/workflows/bench.yml/badge.svg)](https://github.com/danvinci/surrealdb/actions/workflows/bench.yml)
[![Interop](https://github.com/danvinci/surrealdb/actions/workflows/interop.yml/badge.svg)](https://github.com/danvinci/surrealdb/actions/workflows/interop.yml)
[![Docs](https://github.com/danvinci/surrealdb/actions/workflows/docs.yml/badge.svg)](https://danvinci.github.io/surrealdb/)
[![codecov](https://codecov.io/gh/danvinci/surrealdb/branch/main/graph/badge.svg)](https://codecov.io/gh/danvinci/surrealdb)

Julia client for [SurrealDB](https://surrealdb.com). Talks to a remote
`surreal` server over WebSocket or HTTP, or runs the database in-process
via `libsurreal`. Same API for both.

Cross-tested against the [official Go](https://github.com/surrealdb/surrealdb.go)
and [Python](https://github.com/surrealdb/surrealdb.py) SDKs: 12 testsets
ported from `surrealdb.go/db_test.go` run on every CI cycle, and an
interop harness writes fixtures from one SDK and reads them with the
others (Python ↔ Julia, Julia → Go) to catch serialization drift across
implementations. Tested against SurrealDB v2 and v3.

## Quickstart

```julia
using SurrealDB

db = SurrealDB.connect("ws://localhost:8000";
                       ns="test", db="test",
                       auth=SurrealDB.RootAuth("root", "root"))

alice = SurrealDB.create(db, "user", Dict("name" => "Alice", "age" => 30))
users = SurrealDB.select(db, "user")
SurrealDB.update(db, alice["id"], Dict("age" => 31))
SurrealDB.delete(db, alice["id"])

results = SurrealDB.query(db, "SELECT * FROM user WHERE age > 18")

SurrealDB.close!(db)
```

### Do-block form

```julia
SurrealDB.connect("ws://localhost:8000"; ns="test", db="test") do db
    SurrealDB.query(db, "SELECT * FROM stream")
end
```

The client is closed in a `finally`, including on exception.

## Connection modes

| URL scheme | Backend | Notes |
|---|---|---|
| `ws://`, `wss://` | Remote WS | Live queries, sessions, transactions, ping, auto-reconnect |
| `http://`, `https://` | Remote HTTP | Stateless. `live()` raises `UnsupportedFeatureError` (method dispatch) |
| `mem://` | Embedded | In-memory, in-process via `libsurreal` |
| `surrealkv://path` | Embedded | File-backed, in-process |

Embedded mode needs the `libsurreal` shared library:

```julia
SurrealDB.libsurreal_load!("/path/to/libsurreal.dylib")  # or set $SURREALDB_LIB
db = SurrealDB.connect("mem://")
```

## Auth

```julia
SurrealDB.signin!(db, SurrealDB.RootAuth("root", "root"))

SurrealDB.signin!(db, SurrealDB.NamespaceAuth("ns", "user", "pass"))

SurrealDB.signin!(db, SurrealDB.ScopedAuth("ns", "db", "account",
                                           Dict("email" => "a@b.c", "pass" => "x")))

SurrealDB.authenticate!(db, jwt_token)

SurrealDB.invalidate!(db)
```

If the WebSocket drops mid-session, the SDK re-issues `use ns/db` and
`authenticate(token)` on reconnect before flipping `status` back to
`:connected`.

## Typed responses (StructTypes.jl)

```julia
using StructTypes
struct User
    id::SurrealDB.RecordID
    name::String
    age::Int
end
StructTypes.StructType(::Type{User}) = StructTypes.Struct()

users = SurrealDB.select(db, User, "user")
alice = SurrealDB.create(db, User, "user",
                         Dict("name" => "Alice", "age" => 30))
```

`RecordID`, `Date{Time}`, `UUID` round-trip automatically. Nested
`Dict`/`Vector` recurse into nested structs.

## Tables.jl

Query results conform to `Tables.jl` (rows and columns):

```julia
using DataFrames
result = SurrealDB.query_table(db, "SELECT name, age FROM user")
df = DataFrame(result)
```

`query_one(db, sql)` asserts a single statement and returns one table.
`query_table(db, sql)` keeps multi-statement boundaries on remote (the
embedded backend collapses them).

## Live queries

```julia
sub = SurrealDB.live(db, "user")
@async for n::SurrealDB.LiveNotification in sub
    @info "live event" action=n.action record=n.record data=n.result
end

SurrealDB.kill!(sub)
```

Each notification is a [`LiveNotification`](@ref) with typed fields
(`action`, `query_id`, `record`, `result`, `session`); it also subtypes
`AbstractDict` so legacy `n["action"]` access keeps working. After a
reconnect the SDK re-issues `LIVE SELECT` and overwrites `sub.query_id`
with the new server-assigned UUID, so caller-held handles keep working.

## Running functions

```julia
SurrealDB.run(db, "type::is::array", [[1, 2, 3]])  # → true
SurrealDB.run(db, "fn::greet", ["world"])           # user-defined function
```

## Sessions and transactions

```julia
SurrealDB.begin!(db)
try
    SurrealDB.create(db, "user", Dict("name" => "Bob"))
    SurrealDB.commit!(db)
catch
    SurrealDB.cancel!(db)
    rethrow()
end

SurrealDB.let!(db, "min_age", 18)
SurrealDB.query(db, "SELECT * FROM user WHERE age >= \$min_age")
SurrealDB.unset!(db, "min_age")
```

## Graph traversal (MetaGraphsNext)

`to_metagraph` loads via a Pkg extension when `MetaGraphsNext` and
`Graphs` are present:

```julia
using MetaGraphsNext, Graphs
g = SurrealDB.to_metagraph(db,
        "SELECT id, name FROM user",
        "SELECT id, in, out FROM follows")
```

Vertex labels are `RecordID` strings; vertex and edge data are field
dicts. The SDK does not auto-coerce results into a graph; call
`to_metagraph` when you want one.

## Errors

```
SurrealDBError (alias: SurrealError)
├── ServerError          (server-reported, kind-tagged)
│   ├── ValidationError      .parameter_name, .is_parse_error
│   ├── ConfigurationError   .is_live_query_not_supported
│   ├── ThrownError
│   ├── QueryError           .is_timed_out, .is_cancelled
│   ├── SerializationError   .is_deserialization
│   ├── NotAllowedError      .is_token_expired, .is_invalid_auth, .method_name
│   ├── NotFoundError        .table_name, .record_id, .namespace_name
│   ├── AlreadyExistsError   .table_name, .record_id
│   └── InternalError
├── RPCError                  (legacy / unknown JSON-RPC code)
├── ConnectionError           (transport-level: drop, timeout)
├── ConnectionUnavailableError
├── UnsupportedEngineError    .scheme
├── UnsupportedFeatureError   .feature, .transport
├── UnexpectedResponseError
└── EmbeddedFFIError          .op, .message
```

```julia
try
    SurrealDB.create(db, "user:alice", Dict(...))
catch e::SurrealDB.AlreadyExistsError
    @info "already exists" table=e.table_name record=e.record_id
catch e::SurrealDB.ServerError
    @warn "server failure" e
end
```

The wire-format `kind` field maps to the Julia subtype. Legacy servers
that emit only a JSON-RPC `code` go through a code-to-kind table.

## Reconnect

WebSocket connections auto-reconnect on drop. Tune via `connect` kwargs:

```julia
db = SurrealDB.connect("ws://localhost:8000";
    reconnect = true,            # set false to disable retries
    reconnect_max_attempts = 10,
    reconnect_base_delay = 0.5,  # exponential
    reconnect_max_delay = 30.0,
    reconnect_jitter = 0.1,      # [0, 1]
    ping_interval = 30.0,        # 0 disables keepalive
)
```

Subscribe to lifecycle:

```julia
ch = SurrealDB.events(db)
@async for ev in ch
    @info "lifecycle" event=ev   # :connecting, :connected, :reconnecting, :disconnected
end
```

`:connected` fires after state replay (`use!`, `authenticate!`, live
re-subscription) finishes, not before.

## Debugging

RPC traces emit on Julia's `@debug` channel. Enable with
`JULIA_DEBUG=SurrealDB`:

```
┌ Debug: SurrealDB ws RPC → rid=2 method=use params=Any["test", "test"]
┌ Debug: SurrealDB ws RPC ← rid=2 has_error=false
```

## Requirements

- Julia 1.9 or newer
- Remote: SurrealDB server v2.x or v3.x
- Embedded: `libsurreal` from [`surrealdb/surrealdb.c`](https://github.com/surrealdb/surrealdb.c)

## Testing

```bash
julia --project=. test/runtests.jl
```

The suite has three layers:

- **Unit** (no network): types, error parser, FFI marshalling,
  reconnect state machine, integration tests against an in-process
  mock WebSocket server.
- **Integration** (needs `surreal start --bind 127.0.0.1:8001`):
  connection lifecycle, auth, query, methods, sessions, live queries.
- **Embedded** (needs `libsurreal`): full FFI roundtrip.

Layers self-skip when their prerequisite is missing.
