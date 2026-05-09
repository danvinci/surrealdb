# Property-style fuzz tests for the error parser.
#
# Goal: assert that for any payload shape — well-formed, malformed, deeply
# nested, oversized, NUL-byte-laden — the parser entry points either return
# a typed Julia exception subtype, or raise a SurrealDBError. They must
# never:
#   - throw an unrelated exception (e.g. a KeyError or InexactError leaking
#     out from a missed type guard);
#   - return a non-exception sentinel that the caller mistakes for a result.
#
# Pure unit, no network. Catches type guard regressions early.

using SurrealDB
using SurrealDB: _parse_rpc_error, _parse_query_error, _resolve_kind,
    _create_server_error, _CODE_TO_KIND
using Test
using Random

const KINDS = ["Validation", "Configuration", "Thrown", "Query",
               "Serialization", "NotAllowed", "NotFound", "AlreadyExists",
               "Internal"]

# Generate a random "value" for embedding in JSON-like dicts. Includes
# NUL bytes, very long strings, deep nesting, and odd Unicode.
function _random_value(rng::AbstractRNG, depth::Int=0)
    depth >= 4 && return rand(rng, ("leaf", 42, true, nothing))
    pick = rand(rng, 1:8)
    return if pick == 1
        randstring(rng, rand(rng, 0:200))
    elseif pick == 2
        rand(rng, -10_000:10_000)
    elseif pick == 3
        rand(rng) * 1e6 - 5e5
    elseif pick == 4
        rand(rng, (true, false, nothing))
    elseif pick == 5
        # NUL bytes + Unicode oddities. Build from a Char alphabet via join
        # (avoids the Julia 1.10+ Vector{Char}→String constructor pitfall).
        alphabet = collect("abcdefghijklm\0αβγ✓中")
        n = rand(rng, 0:50)
        n == 0 ? "" : join(rand(rng, alphabet, n))
    elseif pick == 6
        Any[_random_value(rng, depth+1) for _ in 1:rand(rng, 0:6)]
    elseif pick == 7
        Dict{String, Any}(randstring(rng, rand(rng, 1:8)) => _random_value(rng, depth+1)
                          for _ in 1:rand(rng, 0:5))
    else
        # Empty
        rand(rng, ("", Any[], Dict{String,Any}()))
    end
end

function _random_rpc_err(rng::AbstractRNG)
    err = Dict{String, Any}()
    if rand(rng) < 0.85
        err["code"] = rand(rng) < 0.6 ?
            rand(rng, keys(_CODE_TO_KIND)) :
            rand(rng, -100_000:100_000)
    end
    if rand(rng) < 0.5
        err["kind"] = rand(rng) < 0.85 ?
            rand(rng, KINDS) :
            randstring(rng, rand(rng, 1:20))
    end
    if rand(rng) < 0.7
        err["message"] = _random_value(rng)
    end
    if rand(rng) < 0.5
        err["details"] = _random_value(rng)
    end
    return err
end

function _random_query_err(rng::AbstractRNG)
    item = Dict{String, Any}("status" => "ERR")
    if rand(rng) < 0.7
        item["result"] = _random_value(rng)
    end
    if rand(rng) < 0.5
        item["kind"] = rand(rng) < 0.85 ?
            rand(rng, KINDS) :
            randstring(rng, rand(rng, 1:20))
    end
    if rand(rng) < 0.5
        item["details"] = _random_value(rng)
    end
    if rand(rng) < 0.3
        item["time"] = randstring(rng, rand(rng, 1:10))
    end
    return item
end

@testset "_parse_rpc_error: never leaks non-SurrealDBError" begin
    rng = Xoshiro(0x5117_eaceb_a571_e57)
    for _ in 1:1000
        err = _random_rpc_err(rng)
        e = try
            _parse_rpc_error(err)
        catch caught
            # Anything escaping the parser MUST be a SurrealDBError. A bare
            # KeyError / MethodError leaking out is a real bug.
            @test caught isa SurrealDBError
            continue
        end
        # Returned value must be an exception subtype callers can `throw`.
        @test e isa SurrealDBError
    end
end

@testset "_parse_query_error: never leaks non-SurrealDBError" begin
    rng = Xoshiro(0xc1eaf_c0fee_eba51)
    for _ in 1:1000
        item = _random_query_err(rng)
        e = try
            _parse_query_error(item)
        catch caught
            @test caught isa SurrealDBError
            continue
        end
        @test e isa SurrealDBError
    end
end

@testset "_create_server_error: any string kind returns ServerError" begin
    rng = Xoshiro(0xc0ffee_facad_e_fac1)
    for _ in 1:500
        kind = randstring(rng, rand(rng, 0:30))
        msg = _random_value(rng)
        msg isa AbstractString || (msg = string(msg))
        e = _create_server_error(kind, msg; details=_random_value(rng))
        @test e isa SurrealDB.ServerError
        @test e isa SurrealDBError
    end
end

@testset "_resolve_kind: every input shape returns a String" begin
    rng = Xoshiro(0xd00dface_d00dface)
    for _ in 1:500
        kind = rand(rng, (nothing, "", randstring(rng, rand(rng, 1:10)),
                          rand(rng, KINDS)))
        code = rand(rng, (nothing, rand(rng, keys(_CODE_TO_KIND)),
                          rand(rng, -100_000:100_000)))
        k = _resolve_kind(kind, code)
        @test k isa String
        @test !isempty(k)
    end
end

@testset "double-wrapped details: arbitrary depth tolerated" begin
    # Build a pathological payload where `details` contains a recursive
    # `kind` matching the outer kind, nested N times. _parse_query_error
    # only unwraps once (per JS semantics) but must not infinite-loop or
    # throw on deeper structures.
    rng = Xoshiro(42)
    inner = _random_value(rng)
    for _ in 1:10
        inner = Dict{String, Any}("kind" => "NotAllowed", "details" => inner)
    end
    item = Dict{String, Any}(
        "status" => "ERR", "result" => "deep",
        "kind" => "NotAllowed", "details" => inner,
    )
    e = _parse_query_error(item)
    @test e isa SurrealDB.NotAllowedError
end

@testset "huge payloads don't OOM the parser" begin
    # 100k-character message string, 1k-element details vector. The parser
    # should pass these through without copying them N times.
    alphabet = collect("abcdefghijklmnopqrstuvwxyz0123456789")
    huge_msg = join(rand(alphabet, 100_000))
    huge_details = Dict{String, Any}("xs" => collect(1:1000))
    e = _parse_rpc_error(Dict("code" => 0, "message" => huge_msg,
                               "kind" => "Internal", "details" => huge_details))
    @test e isa SurrealDB.InternalError
    @test length(e.message) == 100_000
end

@testset "NUL bytes in message and detail strings preserved" begin
    msg = "hello\0world\0\0"
    e = _parse_rpc_error(Dict("code" => 0, "message" => msg,
                               "kind" => "Query"))
    @test e isa SurrealDB.QueryError
    @test e.message == msg
end
