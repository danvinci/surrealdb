# WebSocket transport layer — reader, writer, reconnection, ping keepalive, notifications

function _ws_reconnect_loop(conn::RemoteWSConnection)
    # `attempt`: consecutive failures for backoff; resets on success.
    # `ever_connected`: true once any session established — gates :reconnecting emit and retry logic.
    attempt = 0
    ever_connected = false

    while true
        if attempt > conn.reconnect_max_attempts
            break
        end
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
            # `subprotocol = "json"`: v3 requires explicit protocol; v2 inferred it.
            # Without this, v3 accepts the upgrade then drops the first RPC.
            WebSockets.open(conn.url;
                            subprotocol = "json",
                            require_ssl_verification = conn.tls_verify) do ws
                conn.ws = ws
                attempt = 0           # consecutive-failure counter resets
                ever_connected = true

                # Writer + reader must be up before state replay — replay issues RPCs.
                if conn.write_channel === nothing || !isopen(conn.write_channel)
                    conn.write_channel = Channel{String}(32)
                else
                    try; close(conn.write_channel); catch; end
                    conn.write_channel = Channel{String}(32)
                end
                writer = @async _ws_writer_task(conn)
                reader = @async _ws_reader_task(conn)

                # Replay before :connected so observers see a fully-restored session.
                _reconnect_apply_state!(conn)

                _set_status!(conn, :connected)
                _start_pinger!(conn)

                try; wait(reader); catch; end

                # Signal in-flight RPCs so take! returns a typed error instead of hanging.
                _signal_inflight_disconnect!(conn)

                _stop_pinger!(conn)
                try; close(conn.write_channel); catch; end
                try; wait(writer); catch; end
            end
        catch e
            conn.last_error = e
            if !conn.reconnect
                @error "WebSocket connection failed" exception=e
            end
        end
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

    # Re-subscribe live queries. New UUIDs replace old ones; update live_handles
    # so kill!(sub) targets the server-side subscription after reconnect.
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
                # Re-key handle so kill!(sub) targets the new server-side query_id.
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
    # Gate on socket, not status — writes during state replay must go out.
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
                # Force-close so the reader EOFs and _signal_inflight_disconnect! fires.
                @debug "SurrealDB ws writer error; closing socket" exception=e
                try; close(conn.ws); catch; end
                break
            end
        end
    end
end

function _ws_reader_task(conn::RemoteWSConnection)
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

        raw = String(data)  # String() moves bytes — materialize once for parse + logging

        msg = try
            JSON.parse(raw)
        catch
            continue
        end

        if haskey(msg, "id") && msg["id"] !== nothing
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
            @debug "SurrealDB ws notification ←"
            _dispatch_live_notification(conn, msg["result"])
        elseif haskey(msg, "error")
            # Orphan error: no usable id (JSON-RPC parse errors fire before id is parsed).
            # Signal all in-flight RPCs so they fail fast rather than hang until rpc_timeout.
            err = msg["error"]
            println(stderr, "[ws orphan error] $(first(raw, 300))")
            flush(stderr)
            _signal_inflight_with_error!(conn, err)
        else
            println(stderr, "[ws unrecognized] $(first(raw, 300))")
            flush(stderr)
        end
    end
end

# Unblock in-flight RPCs on socket drop with a synthetic transport error.
# Unlike _teardown_channels!, does NOT close notification channels or empty dicts.
function _signal_inflight_disconnect!(conn::RemoteWSConnection)
    # Snapshot under lock, signal after release — put! on a 1-cap channel blocks
    # if the real response already landed; holding lock across put! deadlocks reader.
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

# Forward the server-supplied error to all in-flight RPCs (connection still alive).
# Used for no-id frames where the server can't attribute the error to a specific request.
function _signal_inflight_with_error!(conn::RemoteWSConnection, err)
    channels = lock(conn.lock) do
        chs = collect(values(conn.response_channels))
        empty!(conn.response_channels)
        chs
    end
    payload = Dict("error" => err)
    for ch in channels
        if isopen(ch) && !isready(ch)
            try
                put!(ch, payload)
            catch
            end
        end
    end
    return nothing
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
    timer = conn.pinger_timer
    if timer !== nothing
        try; close(timer); catch; end
    end
    task = conn.pinger_task
    if task !== nothing && !istaskdone(task)
        try
            t_end = time() + 1.0  # 1s cap; normally exits in microseconds
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

# Live-notification discriminator: result.action + result.id both present.
# Two-field check avoids false-matching plain RPC responses with a Dict result.
function _is_live_notification(msg)
    haskey(msg, "result") || return false
    r = msg["result"]
    r isa AbstractDict || return false
    return haskey(r, "action") && haskey(r, "id")
end

function _dispatch_live_notification(conn::RemoteWSConnection, result::AbstractDict)
    query_id = get(result, "id", nothing)
    query_id === nothing && return nothing
    qid = string(query_id)
    action = get(result, "action", "")

    # KILLED frames arrive after kill!(sub) has already torn down the channel;
    # drop them rather than surfacing server-confirmation as a subscriber event.
    action == "KILLED" && return nothing

    notif = LiveNotification(result)
    lock(conn.notification_lock) do
        ch = get(conn.notification_channels, qid, nothing)
        if ch !== nothing && isopen(ch)
            try
                put!(ch, notif)
            catch e
                e isa InvalidStateException || rethrow()
            end
        end
    end
    return nothing
end

# Legacy {method:"notify"} envelope — no production server emits this, kept for compatibility.
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

        # Release lock before put!(write_channel) — holding it across a blocking
        # write deadlocks the reader, which needs the lock to dispatch responses.
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
                if code == -1 && attempt < max_retries  # transport-level error, retry
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
