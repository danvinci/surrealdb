using SurrealDB
using Test
using Sockets

include("setup.jl")

# Probe whether the integration test server (TEST_URL) is reachable. Lets us
# skip — rather than error — server-dependent testsets when running locally
# without a `surreal start` instance. CI sets SURREALDB_URL and the probe
# always succeeds. Without this gate, a dev who runs the suite without a
# server gets 17 confusing "errored" results that look like real failures.
function _server_reachable(url::String; timeout_s::Float64=0.5)
    m = match(r"^(?:ws|wss|http|https)://([^:/]+):?(\d+)?", url)
    m === nothing && return false
    host = m.captures[1]
    port_str = m.captures[2]
    port = port_str === nothing ? 8000 : parse(Int, port_str)
    return try
        sock = nothing
        ok = Ref(false)
        task = @async begin
            try
                sock = Sockets.connect(host, port)
                ok[] = true
            catch
            end
        end
        deadline = time() + timeout_s
        while time() < deadline && !ok[]
            sleep(0.02)
        end
        sock !== nothing && (try; close(sock); catch; end)
        ok[]
    catch
        false
    end
end

const SERVER_AVAILABLE = _server_reachable(TEST_URL)
SERVER_AVAILABLE || @info "Skipping server-dependent tests — no SurrealDB at $(TEST_URL). Set SURREALDB_URL or start `surreal start --bind 127.0.0.1:8001`."

@testset "SurrealDB.jl" begin
    # --- No-server testsets (always run) ---

    @testset "Types" begin
        include("test_types.jl")
    end

    @testset "Public API surface" begin
        include("test_api_surface.jl")
    end

    @testset "Aqua" begin
        include("test_aqua.jl")
    end

    @testset "JET" begin
        include("test_jet.jl")
    end

    # Error parser — synthetic payloads, no network.
    @testset "Errors" begin
        include("test_errors.jl")
    end

    # Property-style fuzz over parser entry points.
    @testset "Fuzz" begin
        include("test_fuzz.jl")
    end

    # Reconnect-path state-machine unit tests, no network.
    @testset "Reconnect" begin
        include("test_reconnect.jl")
    end

    # Reconnect integration: real WS roundtrips against an in-process mock.
    @testset "Reconnect Integration" begin
        include("test_reconnect_integration.jl")
    end

    # Concurrent-load resilience under socket drop. Hermetic via mock WS.
    @testset "Load resilience" begin
        include("test_load_resilience.jl")
    end

    # --- Server-dependent testsets (gated on SERVER_AVAILABLE) ---

    if SERVER_AVAILABLE
        @testset "Connection" begin
            include("test_connection.jl")
        end
        @testset "Auth" begin
            include("test_auth.jl")
        end
        @testset "Methods" begin
            include("test_methods.jl")
        end
        @testset "Query" begin
            include("test_query.jl")
        end
        @testset "Session" begin
            include("test_session.jl")
        end
        @testset "Live" begin
            include("test_live.jl")
        end
        @testset "Integration Gaps" begin
            include("test_integration_gaps.jl")
        end
        # Conformance ports from surrealdb.go's TestSurrealDBSuite.
        @testset "Go SDK Conformance" begin
            include("test_go_conformance.jl")
        end
        @testset "Type round-trip" begin
            include("test_type_roundtrip.jl")
        end
        @testset "JWT expiry" begin
            include("test_jwt_expiry.jl")
        end
    end

    # --- Embedded (requires libsurreal) ---

    if SurrealDB.LibSurreal.is_loaded()
        @testset "Embedded" begin
            include("test_embedded.jl")
        end
        @testset "FFI Types" begin
            include("test_ffi_types.jl")
        end
        @testset "Memory leak (embedded)" begin
            include("test_memory.jl")
        end
    end
end
