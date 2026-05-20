# Embedded connection backend for SurrealDB.jl
# Implements AbstractConnection for in-process SurrealDB via ccall into libsurreal

# --- Embedded connection type ---

"""
    EmbeddedConnection(url)

An embedded SurrealDB connection running in-process via libsurreal.

URL schemes:
- `mem://` or `memory://` — in-memory database (no persistence)
- `surrealkv://path/to/data.skv` — file-backed database

Requires `libsurreal` to be loaded via [`libsurreal_load!`](@ref).
"""
Base.@kwdef mutable struct EmbeddedConnection <: AbstractConnection
    "Opaque sr_surreal_t* from libsurreal; `C_NULL` when disconnected"
    handle::Ptr{Cvoid}
    "Original URL string the connection was created with — `mem://`, `surrealkv://path`, etc."
    path::String
    "Lifecycle: `:connected` / `:disconnected` / `:connecting`"
    status::Symbol
    "Guards `handle` and `live_streams` against concurrent access from CRUD tasks"
    lock::ReentrantLock
    "live query_id → stream handle (sr_stream_t*) — passed to sr_stream_next/sr_stream_kill"
    live_streams::Dict{String, Ptr{Cvoid}}
    "live query_id → LiveSubscription handle — used by `kill!(client, qid)` to flip caller-held state"
    live_handles::Dict{String, LiveSubscription} = Dict{String, LiveSubscription}()
    "Lifecycle-event Channel (mirrors `RemoteConnection.events`). Embedded fires `:connected` once on successful connect and `:disconnected` once on close; no `:reconnecting` because there's no retry loop."
    events::Channel{Symbol} = Channel{Symbol}(8)
end

function Base.show(io::IO, conn::EmbeddedConnection)
    print(io, "EmbeddedConnection(", conn.path, ", ", conn.status, ")")
end

# Best-effort emit; never blocks the caller. Mirrors `_set_status!` for remote.
function _emit_embedded_event!(conn::EmbeddedConnection, ev::Symbol)
    @async try
        isopen(conn.events) && put!(conn.events, ev)
    catch e
        e isa InvalidStateException || rethrow()
    end
    return nothing
end

# --- Connect ---

function embedded_connect(url::String)::EmbeddedConnection
    LibSurreal.ensure_loaded!()
    conn = EmbeddedConnection(
        handle=C_NULL,
        path=url,
        status=:connecting,
        lock=ReentrantLock(),
        live_streams=Dict{String, Ptr{Cvoid}}(),
        live_handles=Dict{String, LiveSubscription}()
    )
    _connect_embedded!(conn, url)
    return conn
end

function SurrealDB._connect_embedded!(conn::EmbeddedConnection, url::String)
    lock(conn.lock)
    try
        endpoint = url
        if startswith(url, "mem://") || url == "memory://"
            endpoint = "mem://"
        end
        conn.handle = LibSurreal.sr_connect(endpoint)
        conn.status = :connected
    finally
        unlock(conn.lock)
    end
    _emit_embedded_event!(conn, :connected)
    return nothing
end

# --- Close ---

function SurrealDB._close_backend!(conn::EmbeddedConnection)
    was_connected = conn.status == :connected
    lock(conn.lock)
    try
        for (_, stream) in conn.live_streams
            LibSurreal.sr_stream_kill(stream)
        end
        empty!(conn.live_streams)
        if conn.handle != C_NULL
            LibSurreal.sr_disconnect(conn.handle)
            conn.handle = C_NULL
        end
        conn.status = :disconnected
    finally
        unlock(conn.lock)
    end
    if was_connected
        _emit_embedded_event!(conn, :disconnected)
        try; close(conn.events); catch; end
    end
    return nothing
end

# --- Scoping ---

function SurrealDB._use_backend!(conn::EmbeddedConnection, ns::String, db_name::String)
    lock(conn.lock)
    try
        LibSurreal.sr_use_ns(conn.handle, ns)
        LibSurreal.sr_use_db(conn.handle, db_name)
    finally
        unlock(conn.lock)
    end
    return nothing
end

# --- RPC dispatch ---

function SurrealDB._embedded_rpc_call(conn::EmbeddedConnection, method::String, params::Vector{Any})
    # Locking is done inside each sr_* call in libsurreal.jl
    if method == "query"
        sql = params[1]
        vars = length(params) > 1 ? params[2] : Dict{String, Any}()
        return LibSurreal.sr_query(conn.handle, sql, vars)
    elseif method == "create"
        return LibSurreal.sr_create(conn.handle, params...)
    elseif method == "select"
        return LibSurreal.sr_select(conn.handle, params...)
    elseif method == "update"
        return LibSurreal.sr_update(conn.handle, params...)
    elseif method == "delete"
        return LibSurreal.sr_delete(conn.handle, params...)
    elseif method == "insert"
        return LibSurreal.sr_insert(conn.handle, params...)
    elseif method == "upsert"
        return LibSurreal.sr_upsert(conn.handle, params...)
    elseif method == "merge"
        return LibSurreal.sr_merge(conn.handle, params...)
    elseif method == "relate"
        return LibSurreal.sr_relate(conn.handle, params...)
    elseif method == "insert_relation"
        return LibSurreal.sr_insert_relation(conn.handle, params...)
    elseif method == "patch"
        # params = [resource, patches::Vector{Dict}, diff_flag]
        # Each patch is {"op" => "add"|"remove"|"replace", "path" => p, "value" => v}.
        # libsurreal exposes one ccall per op kind; iterate and apply each.
        resource = string(params[1])
        patches = params[2]
        last_result = Any[]
        for p in patches
            op = string(get(p, "op", ""))
            path = string(get(p, "path", ""))
            if op == "add"
                last_result = LibSurreal.sr_patch_add(conn.handle, resource, path, get(p, "value", nothing))
            elseif op == "remove"
                last_result = LibSurreal.sr_patch_remove(conn.handle, resource, path)
            elseif op == "replace"
                last_result = LibSurreal.sr_patch_replace(conn.handle, resource, path, get(p, "value", nothing))
            else
                throw(ConnectionError("Unsupported embedded patch op: $op"))
            end
        end
        return last_result
    elseif method == "live"
        resource = _to_string(params[1])
        diff = length(params) > 1 ? params[2] : false
        stream = LibSurreal.sr_select_live(conn.handle, resource)
        key = string(stream)
        conn.live_streams[key] = stream
        return key
    elseif method == "kill"
        query_id = string(params[1])
        if haskey(conn.live_streams, query_id)
            LibSurreal.sr_stream_kill(conn.live_streams[query_id])
            delete!(conn.live_streams, query_id)
        end
        return LibSurreal.sr_kill(conn.handle, query_id)
    elseif method == "version"
        return LibSurreal.sr_version(conn.handle)
    elseif method == "health"
        return LibSurreal.sr_health(conn.handle)
    elseif method == "export"
        return LibSurreal.sr_export_db(conn.handle, params...)
    elseif method == "import"
        return LibSurreal.sr_import_db(conn.handle, params...)
    elseif method == "signin"
        p = params[1]
        if p isa AbstractDict
            scope = get(p, "AC", nothing) !== nothing ? :RECORD :
                    get(p, "DB", nothing) !== nothing ? :DATABASE :
                    get(p, "NS", nothing) !== nothing ? :NAMESPACE : :ROOT
            return LibSurreal.sr_signin(conn.handle, scope,
                string(get(p, "user", "")), string(get(p, "pass", "")),
                string(get(p, "NS", "")), string(get(p, "DB", "")),
                string(get(p, "AC", "")))
        else
            return LibSurreal.sr_signin(conn.handle, :ROOT, string(p), "", "", "", "")
        end
    elseif method == "signup"
        p = params[1]
        if p isa AbstractDict
            return LibSurreal.sr_signup(conn.handle, :RECORD,
                string(get(p, "user", "")), string(get(p, "pass", "")),
                string(get(p, "NS", "")), string(get(p, "DB", "")),
                string(get(p, "AC", "")))
        else
            return LibSurreal.sr_signup(conn.handle, :RECORD, string(p), "", "", "", "")
        end
    elseif method == "authenticate"
        return LibSurreal.sr_authenticate(conn.handle, string(params[1]))
    elseif method == "invalidate"
        return LibSurreal.sr_invalidate(conn.handle)
    elseif method == "let"
        return LibSurreal.sr_set(conn.handle, string(params[1]), length(params) > 1 ? params[2] : nothing)
    elseif method == "unset"
        return LibSurreal.sr_unset(conn.handle, string(params[1]))
    elseif method == "begin"
        return LibSurreal.sr_begin(conn.handle)
    elseif method == "commit"
        return LibSurreal.sr_commit(conn.handle)
    elseif method == "cancel"
        return LibSurreal.sr_cancel(conn.handle)
    else
        throw(ConnectionError("Unsupported embedded method: $method"))
    end
end

# --- Live query polling (called from live.jl) ---

function SurrealDB._poll_embedded_live(conn::EmbeddedConnection, query_id::String, ch::Channel)
    stream = get(conn.live_streams, query_id, nothing)
    if stream === nothing
        return
    end
    try
        while isopen(ch)
            raw = LibSurreal.sr_stream_next(stream)
            raw === nothing && break
            get(raw, "action", "") == "KILLED" && continue
            ln = SurrealDB.LiveNotification(
                string(get(raw, "action", "")),
                string(get(raw, "query_id", query_id)),
                nothing,
                get(raw, "result", nothing),
                nothing,
            )
            try
                put!(ch, ln)
            catch e
                e isa InvalidStateException && break
                rethrow()
            end
        end
    catch e
        # Surface unexpected errors instead of silently dying
        @warn "_poll_embedded_live: terminated on error" query_id exception=(e, catch_backtrace())
    finally
        # Drop the subscription's active flag so consumers know to stop iterating
        sub = get(conn.live_handles, query_id, nothing)
        if sub !== nothing
            sub.active = false
        end
    end
end
