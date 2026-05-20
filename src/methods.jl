# Data manipulation methods for SurrealDB.jl

# Internal representation of a single query statement result.
# `error` widened to ServerError post-D1: _parse_query_error dispatches on
# the wire `kind` field and may return any kind-tagged subclass
# (NotFoundError, AlreadyExistsError, ValidationError, etc.), not just QueryError.
struct _QueryResult
    status::String   # "OK" or "ERR"
    time::String     # execution time string
    result::Any      # the actual query result data
    error::Union{ServerError, Nothing}
end

"""
    query(client, sql::String; vars=Dict{String, Any}())

Execute a raw SurrealQL query with optional parameterized variables.

Returns `Vector{Dict{String, Any}}` — one result dict per statement.

# Examples
```julia
result = SurrealDB.query(db, "SELECT * FROM stream WHERE active = true")
result = SurrealDB.query(db, "SELECT * FROM \$table WHERE x > \$min",
                         vars=Dict("table" => "stream", "min" => 10))
```
"""
function query(client::SurrealClient{C}, sql::String;
               vars=Dict{String, Any}()) where {C<:AbstractConnection}
    raw = _rpc_call(client, "query", Any[sql, vars])
    parsed = _parse_query_results(raw)
    return _extract_query_results(parsed)
end

"""
    query(client, ::Type{T}, sql::String; vars=Dict{String, Any}())

Execute a raw SurrealQL query and map results to typed Julia structs.

`T` must be a type for which `StructTypes.StructType(T)` returns `StructTypes.Struct()`.
Each result row is converted to an instance of `T` via `StructTypes.constructfrom`.

Returns `Vector{T}` — one instance per result row.

# Examples
```julia
struct Stream
    id::SurrealDB.RecordID
    name::String
    mature::Bool
end
StructTypes.StructType(::Type{Stream}) = StructTypes.Struct()

streams = SurrealDB.query(db, Stream, "SELECT * FROM stream")
```
"""
function query(client::SurrealClient{C}, ::Type{T}, sql::String;
               vars=Dict{String, Any}()) where {C<:AbstractConnection, T}
    raw = _rpc_call(client, "query", Any[sql, vars])
    parsed = _parse_query_results(raw)
    results = _extract_query_results(parsed)
    return _results_to_struct(T, results)
end

"""
    query_table(client, sql::String; vars=Dict{String, Any}()) -> Vector{QueryResultTable}

Like [`query`](@ref) but returns one [`QueryResultTable`](@ref) per statement
instead of nested vectors. Lets multi-statement queries preserve their
per-statement structure as Tables.jl-compatible sources.

!!! note "Embedded mode flattens statement boundaries"
    Remote (WS/HTTP) preserves per-statement results. Embedded (libsurreal)
    flattens all rows from all statements into a single result list (the C
    library does not expose statement boundaries). On embedded, `query_table`
    therefore returns a single-element Vector regardless of how many `;`s the
    SQL contains. Run multi-statement queries one at a time on embedded if
    you need per-statement separation.

# Examples
```julia
tables = SurrealDB.query_table(db, "SELECT * FROM stream; SELECT * FROM event")
using DataFrames
streams_df = DataFrame(tables[1])
events_df  = DataFrame(tables[2])
```

See also: [`query`](@ref), [`query_one`](@ref), [`to_table`](@ref).
"""
function query_table(client::SurrealClient{C}, sql::String;
                     vars=Dict{String, Any}()) where {C<:AbstractConnection}
    results = query(client, sql; vars=vars)
    # Remote multi-stmt: Vector{Vector{Dict}}. Embedded / single-stmt: flat Vector{Dict}.
    if isempty(results)
        return QueryResultTable[QueryResultTable(Dict{String, Any}[])]
    end
    if all(r -> r isa AbstractVector, results)
        return QueryResultTable[
            QueryResultTable(Dict{String, Any}[
                Dict{String, Any}(r) for r in stmt if r isa AbstractDict
            ]) for stmt in results
        ]
    elseif all(r -> r isa AbstractDict, results)
        rows = Dict{String, Any}[Dict{String, Any}(r) for r in results]
        return QueryResultTable[QueryResultTable(rows)]
    else
        # Mixed shapes (e.g. INFO FOR DB alongside SELECT) — wrap each element.
        return QueryResultTable[
            r isa AbstractVector ?
                QueryResultTable(Dict{String, Any}[Dict{String, Any}(x) for x in r if x isa AbstractDict]) :
                r isa AbstractDict ?
                QueryResultTable(Dict{String, Any}[Dict{String, Any}(r)]) :
                QueryResultTable(Dict{String, Any}[])
            for r in results
        ]
    end
end

"""
    query_one(client, sql::String; vars=Dict{String, Any}()) -> QueryResultTable

Convenience wrapper around [`query_table`](@ref) for single-statement queries.
Asserts the SQL produced exactly one statement-result and returns its
[`QueryResultTable`](@ref). Throws `ArgumentError` if the query produced zero
or multiple statements.

# Examples
```julia
table = SurrealDB.query_one(db, "SELECT * FROM stream WHERE active = true")
using DataFrames
df = DataFrame(table)
```

See also: [`query`](@ref), [`query_table`](@ref).
"""
function query_one(client::SurrealClient{C}, sql::String;
                   vars=Dict{String, Any}()) where {C<:AbstractConnection}
    tables = query_table(client, sql; vars=vars)
    if length(tables) != 1
        throw(ArgumentError("query_one expected exactly 1 statement-result, got $(length(tables)). Use query_table for multi-statement queries."))
    end
    return tables[1]
end

"""
    create(client, what, data::Dict{String, Any})

Create a new record in the table or with a specific ID.

- `what`: Table name (String or [`Table`](@ref)) or record ID ([`RecordID`](@ref))
- `data`: Record fields as a Dict

Returns the created record(s) as a Dict or Vector of Dicts.
"""
function create(client::SurrealClient{C}, what, data) where {C<:AbstractConnection}
    return _rpc_call(client, "create", Any[_to_string(what), data])
end

"""
    select(client, what) -> Any

Select all records from a table or fetch a specific record by ID.

- `what`: Table name (String or [`Table`](@ref)) or record ID ([`RecordID`](@ref))

Returns a Vector of records when `what` is a table, or a single record (Dict)
when `what` is a record ID. Returns an empty vector / `nothing` when the target
does not exist.

See also: [`create`](@ref), [`update`](@ref), [`delete`](@ref).
"""
function select(client::SurrealClient{C}, what) where {C<:AbstractConnection}
    return _rpc_call(client, "select", Any[_to_string(what)])
end

"""
    update(client, what, data) -> Any

Replace the contents of a record (or every record in a table) with `data`.
Existing fields not present in `data` are removed; for partial updates use
[`merge`](@ref) instead.

- `what`: Table name or record ID
- `data`: Replacement record contents (Dict)

Returns the updated record(s).

See also: [`merge`](@ref), [`upsert`](@ref), [`patch`](@ref).
"""
function update(client::SurrealClient{C}, what, data) where {C<:AbstractConnection}
    return _rpc_call(client, "update", Any[_to_string(what), data])
end

"""
    delete(client, what) -> Any

Delete a record or every record in a table.

- `what`: Table name or record ID

Returns the deleted record(s). Deleting a nonexistent record is not an error
on SurrealDB v3 (returns `nothing`).

See also: [`select`](@ref), [`create`](@ref).
"""
function delete(client::SurrealClient{C}, what) where {C<:AbstractConnection}
    return _rpc_call(client, "delete", Any[_to_string(what)])
end

"""
    insert(client, table, data) -> Any

Insert one or many records into a table. Unlike [`create`](@ref), `data` may be
a single Dict or a Vector of Dicts for batch inserts; ids are auto-assigned
when not provided.

- `table`: Table name (String or [`Table`](@ref))
- `data`: Single Dict or Vector of Dicts

Returns the inserted record(s).

See also: [`create`](@ref), [`insert_relation`](@ref).
"""
function insert(client::SurrealClient{C}, table, data) where {C<:AbstractConnection}
    return _rpc_call(client, "insert", Any[_to_string(table), data])
end

"""
    upsert(client, what, data) -> Any

Create the record if it does not exist, otherwise replace it. Same payload
shape as [`update`](@ref); the difference is that `update` on a missing
record may not create it on all SurrealDB versions.

See also: [`update`](@ref), [`merge`](@ref).
"""
function upsert(client::SurrealClient{C}, what, data) where {C<:AbstractConnection}
    return _rpc_call(client, "upsert", Any[_to_string(what), data])
end

"""
    merge(client, what, data) -> Any

Partially update a record by merging `data` into the existing record. Fields
present in `data` overwrite existing fields; fields absent are preserved
(unlike [`update`](@ref) which replaces the whole record).

- `what`: Table name or record ID
- `data`: Partial record contents (Dict)

See also: [`update`](@ref), [`patch`](@ref).
"""
function merge(client::SurrealClient{C}, what, data) where {C<:AbstractConnection}
    return _rpc_call(client, "merge", Any[_to_string(what), data])
end

"""
    relate(client, from, edge, to; data=nothing) -> Any
    relate(client, rel::Relationship) -> Any

Create a graph edge from `from` to `to` via `edge`. The flat-arg form is the
canonical first-class signature; the [`Relationship`](@ref) form is convenient
for storing/passing pre-built edges.

- `from`: source record id ([`RecordID`](@ref) or `"table:id"` string)
- `edge`: edge table ([`Table`](@ref) or String) or specific edge record id
- `to`: target record id
- `data`: optional Dict of edge fields

Returns the created edge record.

# Examples
```julia
SurrealDB.relate(db, "person:john", "knows", "person:jane";
                 data=Dict("met" => "2024-01-01"))

# Or via the bulk struct
rel = Relationship("person:john", Table("knows"), "person:jane",
                   Dict("met" => "2024-01-01"))
SurrealDB.relate(db, rel)
```

See also: [`insert_relation`](@ref), [`Relationship`](@ref).
"""
function relate(client::SurrealClient{C}, from, edge, to;
                data=nothing) where {C<:AbstractConnection}
    payload = data === nothing ? Dict{String, Any}() : data
    return _rpc_call(client, "relate",
                     Any[_to_string(from), _to_string(edge),
                         _to_string(to), payload])
end

function relate(client::SurrealClient{C}, rel::Relationship) where {C<:AbstractConnection}
    return relate(client, rel.rel_in, rel.relation, rel.rel_out; data=rel.data)
end

"""
    insert_relation(client, relationship::Relationship) -> Any

Insert a graph edge defined by a [`Relationship`](@ref). Equivalent to
SurrealQL `INSERT RELATION INTO <table> { in, out, ... }`.

Differs from [`relate`](@ref) in that `insert_relation` is intended for batch /
seeded edges; `relate` is the canonical first-class edge primitive.

See also: [`relate`](@ref), [`Relationship`](@ref).
"""
function insert_relation(client::SurrealClient{C}, relationship::Relationship) where {C<:AbstractConnection}
    payload = Dict{String, Any}(
        "in" => _to_string(relationship.rel_in),
        "out" => _to_string(relationship.rel_out)
    )
    merge!(payload, relationship.data)
    return _rpc_call(client, "insert_relation",
                     Any[_to_string(relationship.relation), payload])
end

"""
    patch(client, what, patches::Vector{Dict{String, Any}})

Apply JSON Patch operations to a record.

# Examples
```julia
SurrealDB.patch(db, "user:1", [
    Dict("op" => "replace", "path" => "/name", "value" => "New Name"),
    Dict("op" => "add", "path" => "/tags/0", "value" => "new-tag")
])
```
"""
function patch(client::SurrealClient{C}, what, patches) where {C<:AbstractConnection}
    return _rpc_call(client, "patch", Any[_to_string(what), patches, true])
end

"""
    patch_add(client, what, path::String, value)

Convenience wrapper around [`patch`](@ref) that issues a single
JSON-Patch `add` operation at `path`. `what` is a table name, [`Table`](@ref),
or [`RecordID`](@ref).
"""
function patch_add(client::SurrealClient{C}, what, path::String, value) where {C<:AbstractConnection}
    return patch(client, what, [Dict("op" => "add", "path" => path, "value" => value)])
end

"""
    run(client, fn_name::String, args=Any[]; version=nothing) -> Any

Invoke a SurrealDB function — builtin (`type::*`, `string::*`, ...) or
user-defined (`fn::*`).

- `fn_name`: Function name including namespace, e.g. `"fn::greet"` or `"type::is::array"`
- `args`: Positional args as a `Vector`
- `version`: Optional semver string for versioned user functions

# Examples
```julia
SurrealDB.run(db, "type::is::array", [[1, 2, 3]])  # → true
SurrealDB.run(db, "fn::greet", ["world"])
```
"""
function run(client::SurrealClient{C}, fn_name::String, args=Any[]; version=nothing) where {C<:AbstractConnection}
    return _rpc_call(client, "run", Any[fn_name, version, args])
end

"""
    patch_remove(client, what, path::String)

Convenience wrapper around [`patch`](@ref) that issues a single
JSON-Patch `remove` operation at `path`.
"""
function patch_remove(client::SurrealClient{C}, what, path::String) where {C<:AbstractConnection}
    return patch(client, what, [Dict("op" => "remove", "path" => path)])
end

"""
    patch_replace(client, what, path::String, value)

Convenience wrapper around [`patch`](@ref) that issues a single
JSON-Patch `replace` operation at `path`.
"""
function patch_replace(client::SurrealClient{C}, what, path::String, value) where {C<:AbstractConnection}
    return patch(client, what, [Dict("op" => "replace", "path" => path, "value" => value)])
end

# --- Internal helpers ---

_to_string(x::String) = x
_to_string(x::Table) = x.name
_to_string(x::RecordID) = string(x)

function _parse_query_results(raw)::Vector{_QueryResult}
    results = _QueryResult[]
    if raw isa AbstractVector
        for item in raw
            if item isa AbstractDict && haskey(item, "status")
                status = item["status"]
                time = get(item, "time", "")
                if status == "ERR"
                    push!(results, _QueryResult(status, time, nothing, _parse_query_error(item)))
                else
                    push!(results, _QueryResult(status, time, get(item, "result", nothing), nothing))
                end
            else
                # Item without status field — treat as raw result
                push!(results, _QueryResult("OK", "", item, nothing))
            end
        end
    end
    return results
end

function _extract_query_results(results::Vector{_QueryResult})::Vector{Any}
    out = Any[]
    for qr in results
        if qr.error !== nothing
            throw(qr.error)
        end
        push!(out, qr.result)
    end
    return out
end

# --- Typed struct conversion (StructTypes.jl) ---

"""
    _results_to_struct(T, results)

Convert query result rows to typed Julia struct `T`.

Handles:
- RecordID conversion (string `"table:id"` → RecordID)
- DateTime conversion (ISO-8601 string → DateTime)
- UUID conversion (string → UUID)
- Nested Dict/Vector → recursive struct mapping
"""
function _results_to_struct(::Type{T}, results::Vector{Any}) where {T}
    mapped = T[]
    for r in results
        if r isa AbstractVector
            for row in r
                push!(mapped, _construct_one(T, row))
            end
        elseif r isa AbstractDict
            push!(mapped, _construct_one(T, r))
        end
    end
    return mapped
end

function _results_to_struct(::Type{T}, results::Any) where {T}
    if results isa AbstractVector
        return _results_to_struct(T, results)
    elseif results isa AbstractDict
        return _construct_one(T, results)
    else
        return results
    end
end

"""
    _construct_one(T, dict)

Construct a single instance of `T` from a Dict, with type normalization
for SurrealDB-specific types.
"""
function _construct_one(::Type{T}, dict::AbstractDict) where {T}
    normalized = _normalize_for_construct(T, dict)
    # StructTypes.constructfrom works on NamedTuple but has a dispatch bug with plain Dict.
    pairs = [Symbol(f) => get(normalized, string(f), nothing) for f in fieldnames(T)]
    nt = (; pairs...)
    return StructTypes.constructfrom(T, nt)
end

"""
    _normalize_for_construct(T, dict)

Preprocess dict values to match the target struct's field types.
Converts:
- `"table:id"` strings → RecordID (when target field is RecordID)
- ISO-8601 strings → DateTime
- UUID strings → UUID
- Nested dicts → structs (when target field is a struct type)
"""
function _normalize_for_construct(::Type{T}, dict) where {T}
    field_types = Dict{String, Any}()
    try
        for (name, typ) in zip(fieldnames(T), fieldtypes(T))
            field_types[string(name)] = typ
        end
    catch e
        # fieldnames/fieldtypes throw on abstract T; anything else is a real bug.
        e isa Union{ArgumentError, MethodError} || rethrow()
    end

    normalized = Dict{String, Any}()
    for (k, v) in dict
        ks = string(k)
        target_type = get(field_types, ks, nothing)
        if target_type !== nothing
            normalized[ks] = _coerce_value(v, target_type)
        else
            normalized[ks] = v
        end
    end
    return normalized
end

"""
    _coerce_value(value, target_type)

Coerce a raw value from JSON to the target Julia type.
"""
function _coerce_value(value, ::Type{<:RecordID})
    if value isa String && occursin(':', value)
        return RecordID(value)
    elseif value isa AbstractDict && haskey(value, "tb") && haskey(value, "id")
        return RecordID(string(get(value, "tb", "")), get(value, "id", ""))
    end
    return value
end

function _coerce_value(value, ::Type{DateTime})
    if value isa String
        try
            return DateTime(value)
        catch
            return value
        end
    end
    return value
end

function _coerce_value(value, ::Type{UUIDs.UUID})
    if value isa String
        try
            return UUIDs.UUID(value)
        catch
            return value
        end
    end
    return value
end

function _coerce_value(value, ::Type{T}) where {T}
    if value isa AbstractDict && _is_struct_type(T)
        return _construct_one(T, value)
    elseif value isa AbstractVector && T <: AbstractVector
        return [_coerce_value(x, eltype(T)) for x in value]
    end
    return value
end

_is_struct_type(::Type{T}) where {T} = try
    StructTypes.StructType(T) == StructTypes.Struct()
catch
    false
end

# Typed select
"""
    select(client, ::Type{T}, what)

Select a record or table and map results to typed Julia struct `T`.

For table selects, returns `Vector{T}`.
For record selects, returns `T`.

# Examples
```julia
struct Stream
    id::SurrealDB.RecordID
    name::String
end
StructTypes.StructType(::Type{Stream}) = StructTypes.Struct()

all_streams = SurrealDB.select(db, Stream, "stream")
one_stream = SurrealDB.select(db, Stream, "stream:abc")
```
"""
function select(client::SurrealClient{C}, ::Type{T}, what) where {C<:AbstractConnection, T}
    result = select(client, what)
    if result isa AbstractVector
        return _results_to_struct(T, result)
    elseif result isa AbstractDict
        return _construct_one(T, result)
    else
        return result
    end
end

# Typed create
"""
    create(client, ::Type{T}, what, data)

Create a record and return it as a typed struct `T`.

# Examples
```julia
stream = SurrealDB.create(db, Stream, "stream", Dict("name" => "new"))
```
"""
function create(client::SurrealClient{C}, ::Type{T}, what, data) where {C<:AbstractConnection, T}
    result = create(client, what, data)
    if result isa AbstractDict
        return _construct_one(T, result)
    elseif result isa AbstractVector
        return _results_to_struct(T, result)
    end
    return result
end

# --- Transactions ---

"""
    begin!(client)

Start a new transaction. All subsequent operations within the transaction
are isolated until [`commit!`](@ref) or [`cancel!`](@ref) is called.

On SurrealDB v3+ remote connections, `begin!`/`commit!`/`cancel!` RPC methods
may return "Expected transaction UUID". Use raw SurrealQL instead:
```julia
SurrealDB.query(db, \"BEGIN TRANSACTION; ...; COMMIT TRANSACTION;\")
```
"""
function begin!(client::SurrealClient{C}) where {C<:AbstractConnection}
    _rpc_call(client, "begin", Any[])
    return nothing
end

"""
    commit!(client)

Commit the active transaction, persisting all changes made within it.
For v3+ remote connections, prefer raw SurrealQL `COMMIT TRANSACTION;`.
"""
function commit!(client::SurrealClient{C}) where {C<:AbstractConnection}
    _rpc_call(client, "commit", Any[])
    return nothing
end

"""
    cancel!(client)

Cancel / rollback the active transaction, discarding all changes made within it.
For v3+ remote connections, prefer raw SurrealQL `CANCEL TRANSACTION;`.
"""
function cancel!(client::SurrealClient{C}) where {C<:AbstractConnection}
    _rpc_call(client, "cancel", Any[])
    return nothing
end

# --- Tables.jl integration ---

"""
    QueryResultTable

A Tables.jl-compatible wrapper around query results. Stores rows as a
`Vector{Dict{String, Any}}` and delegates Tables.jl operations to
`Tables.dictrowtable` / `Tables.dictcolumntable` so columns are derived
lazily on access (no eager materialization). Use [`to_table`](@ref) to
construct from raw query results.

# Examples
```julia
result = SurrealDB.query(db, "SELECT * FROM stream")
table = SurrealDB.to_table(result)
using DataFrames
df = DataFrame(table)        # column-major access
for row in Tables.rows(table)  # row-major iteration
    println(row)
end
```
"""
struct QueryResultTable
    rows::Vector{Dict{String, Any}}
end

"""
    to_table(results) -> QueryResultTable

Convert query results to a Tables.jl-compatible source. Can be passed
directly to `DataFrame()` from DataFrames.jl, `Tables.columntable`, etc.

`results` can be:
- A `Vector{Dict{String, Any}}` (single-statement result rows)
- A `Vector{Any}` containing nested vectors of dicts (multi-statement; rows
  are flattened across all statements)
- A single `Dict` (treated as a one-row table)
"""
function to_table(results)
    rows = _flatten_results(results)
    return QueryResultTable(rows)
end

function _flatten_results(results)
    rows = Dict{String, Any}[]
    if results isa AbstractDict
        push!(rows, Dict{String, Any}(results))
        return rows
    end
    for r in results
        if r isa AbstractVector
            for row in r
                if row isa AbstractDict
                    push!(rows, Dict{String, Any}(row))
                end
            end
        elseif r isa AbstractDict
            push!(rows, Dict{String, Any}(r))
        end
    end
    return rows
end

# Tables.jl interface — delegate to Tables.dict{row,column}table for laziness.
# We satisfy BOTH rowaccess and columnaccess (LibPQ.jl convention) so callers
# can pick whichever access pattern is cheaper for their use case.
Tables.istable(::Type{QueryResultTable}) = true

Tables.rowaccess(::Type{QueryResultTable}) = true
Tables.rows(t::QueryResultTable) = Tables.rows(Tables.dictrowtable(t.rows))

Tables.columnaccess(::Type{QueryResultTable}) = true
Tables.columns(t::QueryResultTable) = Tables.columns(Tables.dictcolumntable(t.rows))

Tables.schema(t::QueryResultTable) = isempty(t.rows) ?
    Tables.Schema(Symbol[], Type[]) :
    Tables.schema(Tables.dictrowtable(t.rows))

# Convenience accessors — back-compat shims for callers that used the old
# column-major fields directly. Keep these tiny; prefer `Tables.columns(t)`.
Base.length(t::QueryResultTable) = length(t.rows)
Base.isempty(t::QueryResultTable) = isempty(t.rows)
Base.iterate(t::QueryResultTable, args...) = iterate(t.rows, args...)

function Base.show(io::IO, t::QueryResultTable)
    n = length(t.rows)
    cols = isempty(t.rows) ? String[] : sort(collect(keys(first(t.rows))))
    print(io, "QueryResultTable(", n, " row", n == 1 ? "" : "s",
              isempty(cols) ? "" : ", cols=" * join(cols, ","), ")")
end

# REPL-friendly preview: header + up to 5 rows, truncated columns.
function Base.show(io::IO, ::MIME"text/plain", t::QueryResultTable)
    n = length(t.rows)
    if n == 0
        print(io, "QueryResultTable: 0 rows")
        return
    end
    cols = sort(collect(keys(first(t.rows))))
    println(io, "QueryResultTable: ", n, " row", n == 1 ? "" : "s")
    println(io, "  columns: ", join(cols, ", "))
    preview_n = min(n, 5)
    for i in 1:preview_n
        row = t.rows[i]
        cells = [string(c, "=", repr(get(row, c, missing))) for c in cols]
        println(io, "  [", i, "] ", join(cells, ", "))
    end
    n > preview_n && print(io, "  ⋮ ", n - preview_n, " more row",
                                  n - preview_n == 1 ? "" : "s")
end

