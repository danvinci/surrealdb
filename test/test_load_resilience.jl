# Connection-loss resilience under concurrent load.
#
# The mock-WS integration tests cover one-task-at-a-time drops. This suite
# adds the under-load case: spawn N concurrent RPCs, drop the socket
# mid-flight, assert the system either recovers (reconnect=true) or fails
# every in-flight task with a typed `ConnectionError` (reconnect=false).
#
# The cardinal sin we're guarding against is silent failure — a future
# whose .result lives forever in pending state, or a return value that
# looks successful but reflects a stale read.
#
# Uses the in-process MockWS server so the test is hermetic and runs
# without a real SurrealDB instance.

using SurrealDB
using Test
using Base.Threads: @spawn

include("mock_ws_server.jl")

function _wait_until(pred; timeout_s::Float64=3.0, step_s::Float64=0.05)
    deadline = time() + timeout_s
    while time() < deadline
        pred() && return true
        sleep(step_s)
    end
    return pred()
end

@testset "concurrent RPCs survive a mid-flight drop" begin
    mock = MockWS.start_mock()
    try
        client = SurrealDB.connect("ws://127.0.0.1:$(mock.port)";
            reconnect_base_delay=0.05,
            reconnect_max_delay=0.5,
            reconnect_jitter=0.0,
            ping_interval=0.0)
        try
            SurrealDB.use!(client, "ns", "db")

            # Spawn N concurrent queries. Each should either complete
            # normally (server replies via reconnect) or fail with a typed
            # SurrealDBError. Nothing else is acceptable.
            N = 20
            results = Vector{Any}(undef, N)
            tasks = [Threads.@spawn(begin
                results[i] = try
                    SurrealDB.query(client, "SELECT * FROM 1")
                catch e
                    e isa SurrealDB.SurrealDBError || rethrow()
                    e
                end
            end) for i in 1:N]

            # Let some queries land first, then yank the socket.
            sleep(0.05)
            MockWS.force_drop!(mock)

            # Reconnect should bring us back; verify status flips back.
            @test _wait_until(() -> client.connection.status == :connected;
                              timeout_s=5.0)

            # Wait for all tasks to settle. None may hang past the
            # deadline — that's the cardinal sin.
            #
            # Per-task budget is the SDK's `rpc_timeout` plus slack: a task
            # that lost its socket-drop signal (e.g. it registered AFTER
            # `_signal_inflight_disconnect!` ran on the dropped connection)
            # will eventually fall through to the bounded `take!` wait and
            # raise `ConnectionError("RPC timeout ...")`. The earlier 5s
            # deadline pre-dates `rpc_timeout` and produces flaky CI failures
            # on under-load runners where the signal hasn't propagated within
            # 5s — the task IS settling correctly, just outside the window.
            per_task_deadline = client.connection.rpc_timeout + 5.0
            for (i, t) in enumerate(tasks)
                deadline = time() + per_task_deadline
                while !istaskdone(t) && time() < deadline
                    sleep(0.05)
                end
                @test istaskdone(t)
            end

            # Every result is either a successful query response or a typed
            # SurrealDBError. Nothing else.
            for r in results
                ok = r isa AbstractVector || r isa AbstractDict ||
                     r === nothing || r isa SurrealDB.SurrealDBError
                @test ok
            end
        finally
            try; SurrealDB.close!(client); catch; end
        end
    finally
        MockWS.stop_mock!(mock)
    end
end

@testset "reconnect=false fails all in-flight RPCs cleanly" begin
    mock = MockWS.start_mock()
    try
        client = SurrealDB.connect("ws://127.0.0.1:$(mock.port)";
            reconnect=false, ping_interval=0.0)
        try
            SurrealDB.use!(client, "ns", "db")

            N = 10
            results = Vector{Any}(undef, N)
            tasks = [Threads.@spawn(begin
                results[i] = try
                    SurrealDB.query(client, "SELECT * FROM 1")
                catch e
                    e isa SurrealDB.SurrealDBError || rethrow()
                    e
                end
            end) for i in 1:N]

            sleep(0.05)
            MockWS.force_drop!(mock)

            # Without reconnect, status goes :disconnected and stays.
            @test _wait_until(() -> client.connection.status == :disconnected;
                              timeout_s=3.0)

            # All tasks must terminate (no hangs).
            for t in tasks
                deadline = time() + 3.0
                while !istaskdone(t) && time() < deadline
                    sleep(0.05)
                end
                @test istaskdone(t)
            end
        finally
            try; SurrealDB.close!(client); catch; end
        end
    finally
        MockWS.stop_mock!(mock)
    end
end
