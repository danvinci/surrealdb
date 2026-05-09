# Connection layer — abstract types and remote WebSocket/HTTP backend

# --- Abstract connection type ---

"""
    AbstractConnection

Abstract base type for SurrealDB connection backends.
Concrete implementations: [`RemoteWSConnection`](@ref), [`RemoteHTTPConnection`](@ref) (both `<: AbstractRemoteConnection`), and `EmbeddedConnection` (in `SurrealDB.Embedded`).
"""
abstract type AbstractConnection end

"""
    AbstractRemoteConnection <: AbstractConnection

Common supertype for remote-server backends ([`RemoteWSConnection`](@ref) and
[`RemoteHTTPConnection`](@ref)). Methods that work over either transport
(query, create, select, etc.) dispatch on this; transport-specific methods
(live queries, sessions, pinger) dispatch on the concrete type so HTTP-only
restrictions surface as `MethodError` at the API boundary instead of runtime
`ConnectionError`.
"""
abstract type AbstractRemoteConnection <: AbstractConnection end

# Forward declaration so the const aliases below can reference the parametric
# type. Defined below in full.
"""
    RemoteWSConnection = RemoteConnection{:ws}
    RemoteHTTPConnection = RemoteConnection{:http}

Concrete remote-connection types, parametric on the transport tag (`:ws` or
`:http`). Methods that only apply to one transport dispatch on the alias —
e.g. `live(client::SurrealClient{<:RemoteWSConnection}, ...)`.
"""

# --- Remote connection ---

"""
    RemoteConnection(url)

A remote connection to a SurrealDB server via WebSocket or HTTP.

URL schemes accepted:
- `ws://host:port` / `wss://host:port` — WebSocket (primary, stateful)
- `http://host:port` / `https://host:port` — HTTP (stateless)
"""
Base.@kwdef mutable struct RemoteConnection{P} <: AbstractRemoteConnection
    "URL the client was constructed with — `ws://...`, `wss://...`, `http://...`, or `https://...`"
    url::String = ""
    "Open WebSocket handle (only used when `P == :ws`); `nothing` when disconnected or HTTP"
    ws::Union{Any, Nothing} = nothing
    "Base HTTP URL (only used when `P == :http`)"
    http_base_url::String = ""
    "Guards `request_id` and `response_channels` against concurrent writers"
    lock::ReentrantLock = ReentrantLock()
    "Monotonic counter for JSON-RPC request ids"
    request_id::Int = 0
    "Lifecycle: `:disconnected` / `:connecting` / `:connected` / `:reconnecting`"
    status::Symbol = :disconnected
    "request_id → response Channel; reader task delivers responses via these"
    response_channels::Dict{Int, Channel} = Dict{Int, Channel}()
    "Writer task drains this channel and writes to the WS socket; `nothing` until first connect"
    write_channel::Union{Channel, Nothing} = nothing
    "live query_id → notification Channel"
    notification_channels::Dict{String, Channel} = Dict{String, Channel}()
    "Guards `notification_channels` against concurrent registration/teardown"
    notification_lock::ReentrantLock = ReentrantLock()
    "live query_id → (table, diff) — used by `_reconnect_apply_state!` to re-issue subscriptions on reconnect"
    live_subscriptions::Dict{String, Tuple{String, Bool}} = Dict{String, Tuple{String, Bool}}()
    "live query_id → LiveSubscription handle — used by `kill!(client, qid)` to flip caller-held state"
    live_handles::Dict{String, LiveSubscription} = Dict{String, LiveSubscription}()
    "Background reader task; drains WS messages and dispatches to response/notification channels"
    reader_task::Union{Task, Nothing} = nothing
    "Back-reference to the SurrealClient — used by reconnect to re-apply auth/use!/live state"
    client::Any = nothing
    # --- Reconnection ---
    "If `false`, dropped connections do NOT auto-reconnect"
    reconnect::Bool = true
    "Max consecutive reconnect attempts before giving up; resets to 0 on successful connect"
    reconnect_max_attempts::Int = 10
    "Base delay (seconds) for exponential backoff: delay = base * 2^(attempt - 2)"
    reconnect_base_delay::Float64 = 0.5
    "Cap on the exponential backoff delay (seconds)"
    reconnect_max_delay::Float64 = 30.0
    "Random jitter factor [0, 1] applied to each backoff sleep to avoid thundering-herd"
    reconnect_jitter::Float64 = 0.1
    "Ping keepalive interval (seconds); set to 0 to disable"
    ping_interval::Float64 = 30.0
    "Background ping task; cancelled and replaced on each reconnect"
    pinger_task::Union{Task, Nothing} = nothing
    "Active pinger Timer; closed by `_stop_pinger!` to interrupt the in-flight `wait` and exit the loop"
    pinger_timer::Union{Timer, Nothing} = nothing
    "Lifecycle-event Channel — emits `:connecting` / `:connected` / `:reconnecting` / `:disconnected` / `:error` symbols on state transitions. Subscribe via [`events`](@ref). Drop-in compatible with the JS SDK's `subscribe('connected', ...)` pattern."
    events::Channel{Symbol} = Channel{Symbol}(64)
    "Last exception observed by the reconnect loop. Used to surface a meaningful cause when `connect()` times out instead of a bare \"Failed to connect\" string."
    last_error::Union{Exception, Nothing} = nothing
end

# Concrete protocol-tagged types. Methods that only apply to one transport
# dispatch on these aliases (e.g. `live(::SurrealClient{<:RemoteWSConnection}, ...)`).
const RemoteWSConnection = RemoteConnection{:ws}
const RemoteHTTPConnection = RemoteConnection{:http}

# --- Client struct ---

"""
    SurrealClient{C<:AbstractConnection}

The main client type for interacting with a SurrealDB database.

Generic over the connection backend type `C`. Create via [`connect`](@ref).

# Examples
```julia
db = SurrealDB.connect("ws://localhost:8000")
SurrealDB.use!(db, "test", "test")
result = SurrealDB.query(db, "SELECT * FROM stream")
```
"""
mutable struct SurrealClient{C<:AbstractConnection}
    "Underlying transport — RemoteConnection (WS/HTTP) or EmbeddedConnection"
    connection::C
    "Currently selected namespace; set via `use!` or auto-applied on reconnect"
    namespace::Union{String, Nothing}
    "Currently selected database; set via `use!` or auto-applied on reconnect"
    database::Union{String, Nothing}
    "JWT token from the most recent successful signin/authenticate; `nothing` when unauthenticated"
    token::Union{String, Nothing}
    "Session variables set via `let!` — used for state inspection and reconnect re-application"
    variables::Dict{String, Any}
end

function Base.show(io::IO, c::SurrealClient)
    auth = c.token === nothing ? "unauth" : "auth"
    ns = c.namespace === nothing ? "-" : c.namespace
    db = c.database === nothing ? "-" : c.database
    print(io, "SurrealClient(", _conn_descr(c.connection),
              ", ns=", ns, ", db=", db, ", ", auth, ")")
end

_conn_descr(conn::RemoteWSConnection) = "ws[$(conn.status)]"
_conn_descr(conn::RemoteHTTPConnection) = "http[$(conn.status)]"
_conn_descr(conn::AbstractConnection) = "embedded"

function Base.show(io::IO, conn::RemoteWSConnection)
    print(io, "RemoteWSConnection(", conn.url, ", ", conn.status, ")")
end

function Base.show(io::IO, conn::RemoteHTTPConnection)
    print(io, "RemoteHTTPConnection(", conn.url, ", ", conn.status, ")")
end

# --- URL scheme parsing ---

function _parse_scheme(url::String)
    m = match(r"^(mem(?:ory)?|surrealkv|ws|wss|http|https)://", url)
    if m === nothing
        # Extract the scheme prefix (or report the whole URL if there isn't one)
        scheme = something(match(r"^([a-zA-Z][a-zA-Z0-9+.-]*)://", url), nothing)
        throw(UnsupportedEngineError(scheme === nothing ? url : scheme.captures[1]))
    end
    scheme = m.captures[1]
    if scheme == "memory" || scheme == "mem"
        return :mem
    elseif scheme == "surrealkv"
        return :surrealkv
    elseif scheme == "ws"
        return :ws
    elseif scheme == "wss"
        return :wss
    elseif scheme == "http"
        return :http
    elseif scheme == "https"
        return :https
    end
    # Unreachable: the regex above only matches the schemes above
    throw(ArgumentError("Unsupported URL scheme: $url"))
end

# --- Internal helpers (stubs, filled by agents) ---

# --- WebSocket connect with reconnection ---

"""
    _set_status!(conn::RemoteConnection, status::Symbol)

Update `conn.status` and emit a lifecycle event on `conn.events` when the
status actually changes. Best-effort emission via `@async`: full or closed
channels never block the caller.
"""
function _set_status!(conn::RemoteConnection, status::Symbol)
    old = conn.status
    conn.status = status
    if old != status
        @async try
            isopen(conn.events) && put!(conn.events, status)
        catch e
            e isa InvalidStateException || rethrow()
        end
    end
    return nothing
end

function _connect_remote!(conn::RemoteHTTPConnection)
    # HTTP is stateless — no socket to open, no reader task needed
    _set_status!(conn, :connected)
    return nothing
end

function _connect_remote!(conn::RemoteWSConnection)
    conn.reader_task = @async _ws_reconnect_loop(conn)
    return nothing
end

# --- WebSocket close ---

function _close_remote!(conn::RemoteConnection)
    conn.reconnect = false
    _set_status!(conn, :disconnected)
    # Pinger only exists on the WS transport; HTTP doesn't have one.
    # Short-circuit BEFORE calling _stop_pinger! since that has no method
    # for RemoteHTTPConnection (intentional dispatch tightening from R10).
    if conn isa RemoteHTTPConnection
        return nothing
    end
    _stop_pinger!(conn)
    try
        put!(conn.write_channel, "")
    catch
    end
    try
        close(conn.write_channel)
    catch
    end
    conn.ws = nothing
    return nothing
end

# --- RPC call ---

function _rpc_call_remote(client::SurrealClient{<:RemoteHTTPConnection}, method::String, params::Vector{Any};
                         session=nothing, txn=nothing)
    return _rpc_call_http(client, method, params; session=session, txn=txn)
end

function _rpc_call_remote(client::SurrealClient{<:RemoteWSConnection}, method::String, params::Vector{Any};
                         session=nothing, txn=nothing)
    return _rpc_call_ws(client, method, params; session=session, txn=txn)
end


# --- Scoping / auth helpers (thin wrappers that use conn.client) ---

function _use_remote!(conn::RemoteHTTPConnection, ns::String, db_name::String)
    # HTTP is stateless — ns/db is stored on the client + prepended per query
    return nothing
end

function _use_remote!(conn::RemoteWSConnection, ns::String, db_name::String)
    _rpc_call(conn.client, "use", Any[ns, db_name])
end

function _signin_remote!(conn::RemoteConnection, params)
    _rpc_call(conn.client, "signin", Any[params])
end

function _authenticate_remote!(conn::RemoteConnection, token::String)
    _rpc_call(conn.client, "authenticate", Any[token])
end

function _invalidate_remote!(conn::RemoteConnection)
    _rpc_call(conn.client, "invalidate", Any[])
end

# --- Public API ---

"""
    connect(url::String; ns=nothing, db=nothing, token=nothing, auth=nothing)
    connect(f::Function, url::String; kwargs...)

Connect to a SurrealDB instance. The URL scheme determines the backend:

| URL scheme | Backend | Description |
|---|---|---|
| `ws://host:port` | Remote WS | WebSocket (stateful) |
| `http://host:port` | Remote HTTP | HTTP (stateless) |
| `mem://` | Embedded | In-memory database |
| `surrealkv://path` | Embedded | File-backed database |

Keyword arguments:
- `ns`, `db`: Namespace and database to select after connecting
- `token`: JWT token for authentication
- `auth`: Auth struct ([`RootAuth`](@ref), [`NamespaceAuth`](@ref), etc.) for signin
- `reconnect::Bool=true`: Auto-reconnect on socket drop (WS only)
- `reconnect_max_attempts::Int=10`: Consecutive failures before giving up
- `reconnect_base_delay::Float64=0.5`: Initial backoff (exponential, in seconds)
- `reconnect_max_delay::Float64=30.0`: Cap on the backoff delay
- `reconnect_jitter::Float64=0.1`: Random jitter factor [0,1] applied to each backoff
- `ping_interval::Float64=30.0`: Keepalive cadence; `0` disables

Returns a `SurrealClient{C}` where `C` is the concrete connection backend type.

# Do-block form

The function form mirrors `Base.open`: the client is closed automatically
on exit, even if the block throws.

```julia
SurrealDB.connect("ws://localhost:8000"; ns="test", db="test") do db
    SurrealDB.query(db, "SELECT * FROM stream")
end  # client is closed here
```
"""
function connect(f::Function, url::String; kwargs...)
    client = connect(url; kwargs...)
    try
        return f(client)
    finally
        try; close!(client); catch; end
    end
end

function connect(url::String;
                 ns=nothing, db=nothing, token=nothing, auth=nothing,
                 reconnect::Bool=true,
                 reconnect_max_attempts::Int=10,
                 reconnect_base_delay::Float64=0.5,
                 reconnect_max_delay::Float64=30.0,
                 reconnect_jitter::Float64=0.1,
                 ping_interval::Float64=30.0)
    scheme = _parse_scheme(url)

    if scheme in (:ws, :wss, :http, :https)
        is_http = scheme in (:http, :https)
        is_ws = scheme in (:ws, :wss)

        # Construct the proper URL
        ws_url = url
        http_base = url
        if is_ws && !endswith(url, "/rpc")
            ws_url = rstrip(url, '/') * "/rpc"
        end

        conn = is_http ?
            RemoteHTTPConnection(url=ws_url,
                                 http_base_url=http_base,
                                 response_channels=Dict{Int, Channel}(),
                                 write_channel=nothing,
                                 notification_channels=Dict{String, Channel}(),
                                 reconnect=reconnect,
                                 reconnect_max_attempts=reconnect_max_attempts,
                                 reconnect_base_delay=reconnect_base_delay,
                                 reconnect_max_delay=reconnect_max_delay,
                                 reconnect_jitter=reconnect_jitter,
                                 ping_interval=ping_interval) :
            RemoteWSConnection(url=ws_url,
                               http_base_url=http_base,
                               response_channels=Dict{Int, Channel}(),
                               write_channel=Channel{String}(32),
                               notification_channels=Dict{String, Channel}(),
                               reconnect=reconnect,
                               reconnect_max_attempts=reconnect_max_attempts,
                               reconnect_base_delay=reconnect_base_delay,
                               reconnect_max_delay=reconnect_max_delay,
                               reconnect_jitter=reconnect_jitter,
                               ping_interval=ping_interval)
        _connect_remote!(conn)
        # Wait briefly for WS to establish connection (HTTP is instant)
        if is_ws
            for _ in 1:50
                conn.status == :connected && break
                sleep(0.05)
            end
            if conn.status != :connected
                cause = conn.last_error
                msg = cause === nothing ?
                    "Failed to connect to $ws_url" :
                    "Failed to connect to $ws_url: $(sprint(showerror, cause))"
                throw(ConnectionError(msg, cause))
            end
        end
        client = SurrealClient(conn, nothing, nothing, nothing, Dict{String, Any}())
        conn.client = client

        if auth !== nothing
            signin!(client, auth)
        end
        if token !== nothing
            authenticate!(client, token)
        end
        if ns !== nothing && db !== nothing
            use!(client, ns, db)
        end

        return client
    elseif scheme in (:mem, :surrealkv)
        conn = embedded_connect(url)
        client = SurrealClient(conn, nothing, nothing, nothing, Dict{String, Any}())

        if ns !== nothing && db !== nothing
            use!(client, ns, db)
        end

        return client
    else
        throw(ArgumentError("Unsupported URL scheme: $url"))
    end
end

"""
    close!(client::SurrealClient)

Close the database connection. The client cannot be used after this call.
"""
function close!(client::SurrealClient{C}) where {C<:AbstractConnection}
    _close_backend!(client.connection)
    client.namespace = nothing
    client.database = nothing
    client.token = nothing
    return nothing
end

"""
    status(client::SurrealClient)

Return the current connection status as a Symbol:
`:connected`, `:disconnected`, `:connecting`, `:reconnecting`
"""
function status(client::SurrealClient{C}) where {C<:AbstractConnection}
    return client.connection.status
end

"""
    events(client::SurrealClient{<:AbstractRemoteConnection}) -> Channel{Symbol}

Return a Channel that emits lifecycle event symbols on remote-connection
state transitions: `:connecting`, `:connected`, `:reconnecting`,
`:disconnected`. Drop-in equivalent of the JS SDK's
`db.subscribe('connected', ...)` pattern. Best-effort emission — if no
consumer drains the channel, events are queued (capacity 64) and dropped
silently when the buffer is full.

Only RemoteConnection emits events today; embedded connections have a much
simpler lifecycle (connect succeeds or throws) so the Channel exists but
is never written to.

# Examples
```julia
db = SurrealDB.connect("ws://localhost:8000")
@async for ev in SurrealDB.events(db)
    @info "SurrealDB lifecycle" event=ev status=SurrealDB.status(db)
end
```
"""
function events(client::SurrealClient{C}) where {C<:AbstractRemoteConnection}
    return client.connection.events
end

# Embedded connections have a simpler lifecycle (no reconnect loop) but still
# emit `:connected` on connect and `:disconnected` on close so transport-
# agnostic code can rely on a uniform event stream regardless of backend.
function events(client::SurrealClient{C}) where {C<:AbstractConnection}
    return client.connection.events
end

"""
    use!(client::SurrealClient, ns::String, db::String)

Select a namespace and database for all subsequent operations.
"""
function use!(client::SurrealClient{C}, ns::String, db::String) where {C<:AbstractConnection}
    _use_backend!(client.connection, ns, db)
    client.namespace = ns
    client.database = db
    return nothing
end

"""
    info(client::SurrealClient)

Retrieve database-level information such as tables and schema.

Returns a `Dict{String, Any}`.
"""
function info(client::SurrealClient{C}) where {C<:AbstractConnection}
    return _rpc_call(client, "info", Any[])
end

"""
    version(client::SurrealClient)

Retrieve the SurrealDB server version.

Returns a `NamedTuple` with fields `:version`, `:build`, `:timestamp`.
"""
function version(client::SurrealClient{C}) where {C<:AbstractConnection}
    result = _rpc_call(client, "version", Any[])
    ver = result isa String ? result : get(result, "version", string(result))
    build = result isa Dict ? get(result, "build", "") : ""
    timestamp = result isa Dict ? get(result, "timestamp", "") : ""
    return (version=ver, build=build, timestamp=timestamp)
end

"""
    health(client::SurrealClient)

Check the health of the database connection.

Returns `true` if the database is healthy.
"""
function health(client::SurrealClient{C}) where {C<:AbstractConnection}
    try
        _rpc_call(client, "health", Any[])
        return true
    catch e
        # "method not found" used to surface as RPCError(-32601). Post-D1, the
        # -32601 wire code maps to NotFoundError via _CODE_TO_KIND. Catch both
        # so older servers (legacy RPCError path) and newer servers (kind
        # dispatch) both trigger the trivial-query fallback.
        method_not_found = (e isa NotFoundError) ||
                           (e isa RPCError && e.code == -32601)
        if method_not_found
            try
                _rpc_call(client, "query", Any["SELECT * FROM 1", Dict{String, Any}()])
                return true
            catch
                return false
            end
        end
        return false
    end
end

"""
    export_db(client::SurrealClient, filepath::String)

Export the current namespace and database to a file.
"""
function export_db(client::SurrealClient{C}, filepath::String) where {C<:AbstractConnection}
    _rpc_call(client, "export", Any[filepath])
    return nothing
end

"""
    import_db(client::SurrealClient, filepath::String)

Import data from a file into the current namespace and database.
"""
function import_db(client::SurrealClient{C}, filepath::String) where {C<:AbstractConnection}
    _rpc_call(client, "import", Any[filepath])
    return nothing
end

# --- Backend dispatch (filled by connection + embedded agents) ---

function _rpc_call(client::SurrealClient{C}, method::String, params::Vector{Any};
                   session=nothing, txn=nothing) where {C<:AbstractConnection}
    if C <: RemoteConnection
        return _rpc_call_remote(client, method, params; session=session, txn=txn)
    elseif C <: EmbeddedConnection
        return _embedded_rpc_call(client.connection, method, params)
    else
        throw(ConnectionError("Unknown connection backend: $C"))
    end
end

# Stubs — concrete methods live in connection.jl (RemoteConnection) and
# embedded.jl (EmbeddedConnection, qualified as `SurrealDB._<name>!` so it
# extends these stubs across the module boundary).
function _close_backend! end
function _use_backend! end
function _embedded_rpc_call end
function _connect_embedded! end

function _close_backend!(conn::RemoteConnection)
    _close_remote!(conn)
end

function _use_backend!(conn::RemoteConnection, ns::String, db_name::String)
    _use_remote!(conn, ns, db_name)
end
