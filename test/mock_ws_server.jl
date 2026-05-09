# In-process mock SurrealDB WebSocket server for reconnect integration tests.
#
# Speaks just enough JSON-RPC to make the SDK think it's talking to a real
# server: replies to `signin`, `authenticate`, `use`, `live`, `kill`, `ping`,
# `query`, and unknown methods. Records every message the SDK sends so tests
# can assert reconnect-state replay (use!, signin!, live re-issue).
#
# Scenario controls:
# - `drop_after_n` — close the WebSocket after handling N messages.
# - `reject_first_n` — close TCP without upgrading for the first N connect attempts.
# - `force_drop!(mock)` — programmatically drop the active socket from a test.

module MockWS

using WebSockets, Sockets, JSON, UUIDs

# SDK sends `Sec-WebSocket-Protocol: json` (required by SurrealDB v3+);
# WebSockets.jl rejects upgrades with subprotocols not on the whitelist,
# so register `json` here. Mirror this in any other test harness that
# wraps WebSockets.upgrade.
WebSockets.addsubproto("json")

mutable struct Mock
    port::Int
    listener::Sockets.TCPServer
    serve_task::Union{Task, Nothing}

    # Per-connection scenario knobs. Mutated between scenarios.
    drop_after_n::Int            # 0 = never (well-behaved)
    reject_first_n::Int          # close TCP without WS upgrade for first N

    # Observed state — append-only across the lifetime of the mock.
    connect_attempts::Int        # incremented for every TCP accept (incl. rejected)
    upgrade_count::Int           # incremented for every successful WS upgrade
    messages::Vector{Dict{String, Any}}
    methods_seen::Vector{String}

    # Live-query UUID generator: each `live` RPC gets a fresh UUID, distinct
    # across reconnects so tests can assert handle re-keying.
    live_counter::Int

    # Active WS handle — set by handler, cleared on close. Lets tests trigger
    # an out-of-band drop via force_drop!.
    active_ws::Any

    lock::ReentrantLock
end

function start_mock(; drop_after_n::Int=0, reject_first_n::Int=0)
    listener = listen(IPv4("127.0.0.1"), 0)  # OS-assigned port
    port = Int(getsockname(listener)[2])
    mock = Mock(port, listener, nothing,
                drop_after_n, reject_first_n,
                0, 0, Dict{String, Any}[], String[],
                0, nothing, ReentrantLock())
    mock.serve_task = @async _serve_loop(mock)
    # Give the listener a moment to come up before tests dial it.
    sleep(0.05)
    return mock
end

function stop_mock!(mock::Mock)
    # Close any active WS first so the connection coroutine returns and
    # HTTP.listen's handler unwinds. Then close the listener — its accept
    # loop wakes up with an error which the serve task swallows.
    force_drop!(mock)
    try; close(mock.listener); catch; end
    # Don't `wait(serve_task)`: HTTP.listen's accept loop can stay blocked on
    # a transient handler even after listener close. Tests run sub-second so
    # leaving the task to be reaped at process exit is fine; the listener is
    # closed so no new connections land.
    return nothing
end

function force_drop!(mock::Mock)
    lock(mock.lock) do
        ws = mock.active_ws
        if ws !== nothing
            try; close(ws); catch; end
        end
    end
end

# Test helpers — read observed state under the lock.
methods_seen(mock::Mock) = lock(() -> copy(mock.methods_seen), mock.lock)
connect_attempts(mock::Mock) = lock(() -> mock.connect_attempts, mock.lock)
upgrade_count(mock::Mock) = lock(() -> mock.upgrade_count, mock.lock)

function _serve_loop(mock::Mock)
    try
        WebSockets.HTTP.listen("127.0.0.1", mock.port,
                               server=mock.listener,
                               readtimeout=0) do http
            attempt = lock(mock.lock) do
                mock.connect_attempts += 1
                mock.connect_attempts
            end

            # Refuse the first N connects: close TCP without WS upgrade.
            if attempt <= mock.reject_first_n
                try; close(http.stream); catch; end
                return
            end

            if WebSockets.is_upgrade(http.message)
                WebSockets.upgrade(http) do _req, ws
                    lock(mock.lock) do
                        mock.upgrade_count += 1
                        mock.active_ws = ws
                    end
                    try
                        _handle_connection(mock, ws)
                    finally
                        lock(mock.lock) do
                            mock.active_ws = nothing
                        end
                    end
                end
            end
        end
    catch
        # Listener closed — graceful exit.
    end
end

function _handle_connection(mock::Mock, ws)
    msg_count = 0
    while isopen(ws)
        data, ok = readguarded(ws)
        ok || break
        isempty(data) && continue

        msg_count += 1
        req = try
            JSON.parse(String(data))
        catch
            break
        end
        req isa AbstractDict || break

        lock(mock.lock) do
            push!(mock.messages, req)
            push!(mock.methods_seen, get(req, "method", "?"))
        end

        _reply(mock, ws, req)

        # Drop-after-N: close after the Nth message handled.
        if mock.drop_after_n > 0 && msg_count >= mock.drop_after_n
            try; close(ws); catch; end
            break
        end
    end
end

function _reply(mock::Mock, ws, req::AbstractDict)
    method = get(req, "method", "")
    id = get(req, "id", "")
    result = if method == "signin" || method == "authenticate"
        "mock-jwt-token"
    elseif method == "use"
        nothing
    elseif method == "live"
        # Fresh UUID per call so tests can detect re-issuance after reconnect.
        lock(mock.lock) do
            mock.live_counter += 1
        end
        string(uuid4())
    elseif method == "kill"
        nothing
    elseif method == "ping"
        nothing
    elseif method == "query"
        # Empty result set — enough for the SDK's parser to be happy.
        Any[Dict("status" => "OK", "time" => "0.1ms", "result" => Any[])]
    elseif method == "info"
        Dict{String, Any}()
    else
        nothing
    end

    response = JSON.json(Dict("id" => id, "result" => result))
    writeguarded(ws, response)
end

end # module MockWS
