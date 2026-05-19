# WebSocket transport layer — reader, writer, reconnection, ping keepalive, notifications

function _ws_reconnect_loop(conn::RemoteWSConnection)
    # `attempt` counts consecutive failed attempts for backoff calc; resets on
    # success. `ever_connected` records whether we got a working socket up at
    # least once — used to (a) emit :reconnecting before any retry, not just
    # the second one, and (b) classify a clean drop as "should we retry?"
    # without losing the difference between "first connect failed" and
    # "established session dropped."
    attempt = 0
    ever_connected = false

    while true
        # Stop if we've burned through the retry budget on consecutive failures.
        if attempt > conn.reconnect_max_attempts
            break
        end
        # Stop after a clean drop on a previously-good session if reconnect off.
        if ever_connected && !conn.reconnect && attempt == 0
            break
        end

        # Backoff before any attempt that isn't the very first.
        if ever_connected || attempt > 0
            _set_status!(conn, :reconnecting)
            if attempt > 0
                delay = min(conn.reconnect_base_delay * (2.0 ^ (attempt - 1)),
                            conn.reconnect_max_delay)
                jitter = rand() * conn.reconnect_jitter * delay
                sleep(delay + jitter)
            end
        end

        attempt += 1

        try
            # `subprotocol = "json"` sends `Sec-WebSocket-Protocol: json` on
            # the upgrade. SurrealDB 3.0+ requires the protocol format to be
            # explicit; v2.x inferred it. Without this, v3 servers accept
            # the upgrade then drop the first RPC mid-request (the SDK
            # surfaces it as `RPCError(-1): Connection lost mid-request`).
            #
            # `require_ssl_verification` propagates to HTTP.jl's TLS layer.
            # Defaults to verify on; tests with self-signed certs flip it
            # via the `tls_verify` connect kwarg.
            WebSockets.open(conn.url;
                            subprotocol = "json",
                            require_ssl_verification = conn.tls_verify) do ws
                conn.ws = ws
                attempt = 0           # consecutive-failure counter resets
                ever_connected = true

                # Bring up bidirectional message flow BEFORE state replay —
                # _reconnect_apply_state! issues RPCs that need a running
                # writer + reader to round-trip. Reader runs async so we can
                # call _rpc_call from this same task.
                if conn.write_channel === nothing || !isopen(conn.write_channel)
                    conn.write_channel = Channel{String}(32)
                else
                    try; close(conn.write_channel); catch; end
                    conn.write_channel = Channel{String}(32)
                end
                writer = @async _ws_writer_task(conn)
                reader = @async _ws_reader_task(conn)

                # Replay session state (use!, authenticate!, live re-issue)
                # while writer + reader are live but BEFORE flipping status to
                # :connected — so observers waiting on `events` see :connected
                # only after the session is fully restored.
                _reconnect_apply_state!(conn)

                _set_status!(conn, :connected)
                _start_pinger!(conn)

                # Wait for the reader to exit (socket closed by either end).
                try; wait(reader); catch; end

                # Socket has closed. Any RPCs that were waiting on a
                # response (still in `response_channels`) will hang
                # forever otherwise — push a synthetic transport error
                # so each `take!` wakes up and the caller sees a typed
                # ConnectionError instead of a deadlock.
                _signal_inflight_disconnect!(conn)

                _stop_pinger!(conn)
                try; close(conn.write_channel); catch; end
                try; wait(writer); catch; end
            end
        catch e
            # Either the WS handshake failed or a clean drop returned via
            # the do-block. Either way: record a failed attempt iff we never
            # got past handshake on this iteration. (After a successful
            # connect the do-block resets `attempt = 0`; if it stayed 0 here,
            # the drop was clean — we keep `attempt` at 0 to avoid spuriously
            # delaying the first retry of a stable-then-dropped session.)
            conn.last_error = e
            if !conn.reconnect
                @error "WebSocket connection failed" exception=e
            end
        end

        # Clean drop after a good session: keep `attempt` at 0 so the next
        # loop iteration retries immediately (subject to the reconnect flag
        # check at the top). The early-exit above handles `!conn.reconnect`.
    end

    _stop_pinger!(conn)
    _set_status!(conn, :disconnected)
    conn.ws = nothing
    _teardown_channels!(conn)
    return nothing
end

function _reconnect_apply_state!(conn::RemoteWSConnection)
    client = conn.client
    if client === nothing
        return
    end

    # Re-select namespace/database if previously set
    if client.namespace !== nothing && client.database !== nothing
        try
            _use_remote!(conn, client.namespace, client.database)
        catch
        end
    end

    # Re-authenticate if token exists
    if client.token !== nothing
        try
            _authenticate_remote!(conn, client.token)
        catch
        end
    end

    # Re-subscribe live queries with original parameters. The new server-assigned
    # UUIDs replace the old ones; we MUST also update any caller-held
    # LiveSubscription handles in `live_handles` so that `kill!(sub)` after
    # reconnect targets the live query that actually exists on the server.
    old_subs = copy(conn.live_subscriptions)
    empty!(conn.live_subscriptions)

    old_channels = copy(conn.notification_channels)
    empty!(conn.notification_channels)

    old_handles = copy(conn.live_handles)
    empty!(conn.live_handles)

    for (old_qid, (table, diff)) in old_subs
        ch = get(old_channels, old_qid, nothing)
        if ch !== nothing && isopen(ch)
            try
                result = _rpc_call(client, "live", Any[table, diff])
                new_qid = result isa String ? result : string(result)
                conn.live_subscriptions[new_qid] = (table, diff)
                conn.notification_channels[new_qid] = ch
                # Re-key the LiveSubscription handle and mutate its query_id so
                # callers iterating `sub.channel` continue to receive notifications
                # and `kill!(sub)` (which calls `kill!(client, sub.query_id)`)
                # targets the new server-side subscription.
                sub = get(old_handles, old_qid, nothing)
                if sub !== nothing
                    sub.query_id = new_qid
                    conn.live_handles[new_qid] = sub
                end
            catch
                # Re-subscription failed, close the old channel and mark sub dead
                try; close(ch); catch; end
                sub = get(old_handles, old_qid, nothing)
                if sub !== nothing
                    sub.active = false
                end
            end
        end
    end
end

function _ws_writer_task(conn::RemoteWSConnection)
    # Gate on socket aliveness, not status, so writes during state replay
    # (when status is still :reconnecting) actually go out. The reconnect loop
    # owns status; the writer just pumps bytes while the socket is up.
    while conn.ws !== nothing && isopen(conn.ws)
        msg = try
            take!(conn.write_channel)
        catch e
            # Channel closed externally — exit cleanly.
            break
        end
        if msg == ""
            break
        end
        if conn.ws !== nothing && isopen(conn.ws)
            try
                write(conn.ws, msg)
            catch e
                e isa InvalidStateException && break
                # A write failure swallowed silently would orphan the RPC
                # waiting on the response channel for `msg` — the request
                # never reached the server, but the caller still blocks on
                # take!. Force-close the socket so the reader EOFs and the
                # reconnect loop signals in-flight RPCs with a synthetic
                # transport error.
                @debug "SurrealDB ws writer error; closing socket" exception=e
                try; close(conn.ws); catch; end
                break
            end
        end
    end
end

function _ws_reader_task(conn::RemoteWSConnection)
    # Same rationale as the writer: read until the socket itself reports EOF.
    # This is the canonical "connection ended" signal; status is consumed, not
    # produced, by this task.
    while conn.ws !== nothing && isopen(conn.ws)
        data = try
            read(conn.ws)
        catch e
            if e isa EOFError || e isa Base.IOError
                break
            end
            rethrow()
        end
        isempty(data) && continue

        # `String(::Vector{UInt8})` MOVES the bytes — `data` is empty after the
        # call, so we must materialize once and reuse for both JSON.parse and
        # diagnostic logging.
        raw = String(data)

        msg = try
            JSON.parse(raw)
        catch
            continue
        end

        if haskey(msg, "id")
            rid = msg["id"]
            @debug "SurrealDB ws RPC ←" rid=rid has_error=haskey(msg, "error")
            lock(conn.lock) do
                ch = get(conn.response_channels, rid, nothing)
                if ch !== nothing && isopen(ch)
                    put!(ch, msg)
                end
            end
        elseif get(msg, "method", "") == "notify"
            # Legacy notification envelope: {method:"notify", params:{id, ...}}
            @debug "SurrealDB ws notification ← (legacy)"
            _dispatch_notification(conn, msg)
        elseif _is_live_notification(msg)
            # SurrealDB v2/v3 live-notification envelope: no method, no top-
            # level id, payload at msg.result.{action,id,record,result,session}.
            # Confirmed against test-remote CI logs on d316a6f for both v2
            # and v3 server images.
            @debug "SurrealDB ws notification ←"
            _dispatch_live_notification(conn, msg["result"])
        else
            println(stderr, "[ws unrecognized] $(first(raw, 300))")
            flush(stderr)
        end
    end
end

# Wake every blocked `take!(response_channels[rid])` with a synthetic
# transport error. Used on socket drop (transient OR terminal) so in-flight
# RPCs fail-fast with a typed `ConnectionError` instead of hanging until
# the OS times the underlying socket out — or forever, if the SDK
# reconnects to a different session that never delivers the original rid.
#
# Distinct from `_teardown_channels!` because that one also closes
# notification channels and empties dicts; this just unblocks pending
# requests so `_rpc_call`'s `take!` returns and its `if haskey(response,
# "error")` arm raises.
function _signal_inflight_disconnect!(conn::RemoteWSConnection)
    # Snapshot channels under lock, signal each AFTER release. `put!` on a
    # 1-cap Channel blocks if the channel already holds the real response
    # (race: server delivered just before socket dropped). Holding `conn.lock`
    # across a blocking `put!` deadlocks the reader, which needs the lock to
    # dispatch the take! that would drain the channel.
    channels = lock(conn.lock) do
        chs = collect(values(conn.response_channels))
        empty!(conn.response_channels)
        chs
    end
    for ch in channels
        # If the channel is already full (the real response landed), the
        # caller's take! will succeed normally; no synthetic error needed.
        if isopen(ch) && !isready(ch)
            try
                put!(ch, Dict("error" => Dict("code" => -1,
                    "message" => "Connection lost mid-request")))
            catch
            end
        end
    end
end

function _teardown_channels!(conn::RemoteWSConnection)
    lock(conn.lock) do
        for ch in values(conn.response_channels)
            if isopen(ch)
                try
                    put!(ch, Dict("error" => Dict("code" => -1, "message" => "Connection closed")))
                catch
                end
            end
        end
        empty!(conn.response_channels)
    end
    for ch in values(conn.notification_channels)
        if isopen(ch)
            try; close(ch); catch; end
        end
    end
    empty!(conn.notification_channels)
end

function _start_pinger!(conn::RemoteWSConnection)
    _stop_pinger!(conn)
    client = conn.client
    interval = conn.ping_interval
    interval > 0 || return nothing  # 0 disables ping
    conn.pinger_task = @async begin
        try
            while conn.status == :connected
                # Use a Timer so `_stop_pinger!` can interrupt the wait
                # immediately by closing the timer (otherwise we'd block up to
                # `ping_interval` seconds before noticing the shutdown).
                conn.pinger_timer = Timer(interval)
                try
                    wait(conn.pinger_timer)
                catch e
                    # Timer closed externally (shutdown signal) → exit cleanly
                    e isa EOFError && break
                    rethrow()
                end
                conn.status == :connected || break
                try
                    if client !== nothing
                        _rpc_call(client, "ping", Any[])
                    end
                catch
                    # Ping failed — close socket to trigger reconnection
                    if conn.ws !== nothing
                        try; close(conn.ws); catch; end
                    end
                    break
                end
            end
        finally
            conn.pinger_timer = nothing
        end
    end
    return nothing
end

function _stop_pinger!(conn::RemoteWSConnection)
    # Close the timer to wake the pinger task out of `wait(timer)`. The task
    # then checks status / loop condition and exits cleanly. We wait briefly
    # for the task to finish so callers (e.g. `_close_remote!`) can rely on
    # the pinger being gone.
    timer = conn.pinger_timer
    if timer !== nothing
        try; close(timer); catch; end
    end
    task = conn.pinger_task
    if task !== nothing && !istaskdone(task)
        try
            # 1s cap; in normal operation the task exits in microseconds
            t_end = time() + 1.0
            while !istaskdone(task) && time() < t_end
                yield()
            end
        catch
        end
    end
    conn.pinger_task = nothing
    conn.pinger_timer = nothing
    return nothing
end

function _setup_notification_channel(conn::RemoteWSConnection, query_id::String, ch::Channel)
    lock(conn.notification_lock) do
        conn.notification_channels[query_id] = ch
    end
    return nothing
end

function _teardown_notification_channel(conn::RemoteWSConnection, query_id::String)
    lock(conn.notification_lock) do
        delete!(conn.notification_channels, query_id)
    end
    return nothing
end

# Recognize SurrealDB v2/v3 live-notification frames. Shape:
#   {"result": {"action": "CREATE|UPDATE|DELETE|KILLED",
#               "id": "<live-uuid>", "record": "...",
#               "result": <payload>, "session": "..."}}
# `session` is v3-only; v2 omits it. The discriminator is `result.action` +
# `result.id` together — both must be present so we don't false-match a plain
# RPC response that happens to carry a Dict result with an `id` field.
function _is_live_notification(msg)
    haskey(msg, "result") || return false
    r = msg["result"]
    r isa AbstractDict || return false
    return haskey(r, "action") && haskey(r, "id")
end

# v2/v3 notification dispatch. `result` is the inner dict already unwrapped
# from `msg["result"]` by the reader. The full inner dict is forwarded to the
# subscriber's channel so callers can branch on `action` and inspect `record`
# / `result` / `session` — matches surrealdb.py's "yield full notification"
# behavior (PR #247).
function _dispatch_live_notification(conn::RemoteWSConnection, result::AbstractDict)
    query_id = get(result, "id", nothing)
    query_id === nothing && return nothing
    qid = string(query_id)
    action = get(result, "action", "")

    # KILLED is server confirmation of a kill RPC. `kill!` already tore down
    # the local subscription channel (live.jl), so by the time KILLED lands
    # `notification_channels[qid]` is gone and the lookup below would no-op
    # anyway. Drop explicitly to avoid surfacing it to subscribers who didn't
    # ask for it.
    action == "KILLED" && return nothing

    lock(conn.notification_lock) do
        ch = get(conn.notification_channels, qid, nothing)
        if ch !== nothing && isopen(ch)
            try
                put!(ch, result)
            catch e
                # Channel closed concurrently by `kill!(sub)` between the
                # `isopen` check and `put!`. Drop silently.
                e isa InvalidStateException || rethrow()
            end
        end
    end
    return nothing
end

# Legacy `{method:"notify", params:{id, ...}}` envelope. Kept for protocol
# compatibility in case the server ever falls back to it; current v2/v3 use
# the unwrapped-result shape above. No production server we exercise emits
# this shape, but the routing branch is cheap to keep.
function _dispatch_notification(conn::RemoteWSConnection, notif)
    params = get(notif, "params", Dict{String, Any}())
    query_id = params isa Dict ? get(params, "id", nothing) : nothing
    if query_id === nothing
        println(stderr, "[notif dropped: no id] $(notif)")
        flush(stderr)
        return
    end
    qid = string(query_id)
    lock(conn.notification_lock) do
        ch = get(conn.notification_channels, qid, nothing)
        if ch !== nothing && isopen(ch)
            try
                put!(ch, params)
            catch e
                e isa InvalidStateException || rethrow()
            end
        end
    end
    return nothing
end

function _rpc_call_ws(client::SurrealClient{<:RemoteWSConnection}, method::String, params::Vector{Any};
                      session=nothing, txn=nothing)
    conn = client.connection

    max_retries = 3
    attempt = 0

    while true
        attempt += 1

        # Hold `conn.lock` ONLY for response_channels registration. Releasing
        # before `put!(conn.write_channel, ...)` prevents the writer-blocked
        # -on-full-buffer scenario from deadlocking the reader (which needs
        # the same lock to dispatch the response that would unblock take!).
        rid = 0
        ch = Channel{Any}(1)
        registered = false
        lock(conn.lock) do
            conn.request_id += 1
            rid = conn.request_id
            if conn.write_channel !== nothing && isopen(conn.write_channel)
                conn.response_channels[rid] = ch
                registered = true
            end
        end

        if !registered
            if attempt < max_retries && conn.status == :reconnecting
                sleep(0.5)
                continue
            end
            throw(ConnectionError("No active WebSocket connection (status: $(conn.status))"))
        end

        msg = Dict{String, Any}("id" => rid, "method" => method, "params" => params)
        if session !== nothing
            msg["session"] = string(session)
        end
        if txn !== nothing
            msg["txn"] = string(txn)
        end
        json_msg = JSON.json(msg)
        @debug "SurrealDB ws RPC →" rid=rid method=method params=params
        try
            put!(conn.write_channel, json_msg)
        catch e
            lock(conn.lock) do
                delete!(conn.response_channels, rid)
            end
            if attempt < max_retries
                sleep(0.5)
                continue
            end
            throw(ConnectionError("Failed to send RPC: $e", e))
        end

        # Bounded wait for response. Without this, a request that reaches the
        # server but never gets a reply (server bug, malformed response that
        # fails id-routing, etc.) deadlocks the caller indefinitely.
        response = nothing
        retry_after = false
        deadline = time() + conn.rpc_timeout
        while time() < deadline
            if isready(ch)
                try
                    response = take!(ch)
                catch e
                    if e isa InvalidStateException && attempt < max_retries
                        retry_after = true
                        sleep(0.5)
                    else
                        rethrow()
                    end
                end
                break
            end
            sleep(0.01)
        end
        if response === nothing
            if retry_after
                continue
            end
            lock(conn.lock) do
                delete!(conn.response_channels, rid)
            end
            throw(ConnectionError("RPC timeout after $(conn.rpc_timeout)s waiting for `$method` response"))
        end

        if haskey(response, "error")
            err = response["error"]
            if err isa AbstractDict
                code_raw = get(err, "code", -1)
                code = code_raw isa Integer ? Int(code_raw) : -1
                # Retry on transport-level errors before classifying further.
                if code == -1 && attempt < max_retries
                    sleep(0.5)
                    continue
                end
                throw(_parse_rpc_error(err))
            else
                throw(RPCError(-1, string(err)))
            end
        end

        lock(conn.lock) do
            delete!(conn.response_channels, rid)
        end
        return get(response, "result", nothing)
    end
end
