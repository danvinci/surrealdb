# Cross-SDK interop test: assert SurrealDB.jl reads back what surrealdb-py
# wrote. Run write_test_data.py first to populate the fixtures.
#
# Hard-skips when the interop table is empty (signals that the Python
# writer hasn't been run, or didn't see the same DB).

using SurrealDB
using Test

const URL = get(ENV, "SURREALDB_URL", "ws://localhost:8000")
const NS = get(ENV, "SURREALDB_NS", "test")
const DB = get(ENV, "SURREALDB_DB", "test")

# Expected values, indexed by `kind` field. Each fixture's `value` round-trips
# through SurrealDB's wire format twice (Python encode → DB → Julia decode);
# we assert the Julia-decoded result equals the Python source value.
const EXPECTED = Dict(
    "int_positive"   => 12345,
    "int_negative"   => -67890,
    "float_simple"   => 3.14159,
    "string_ascii"   => "hello world",
    "string_unicode" => "αβγ ✓ 中文 🦀",
    "bool_true"      => true,
    "bool_false"     => false,
    "null_value"     => nothing,
    "array_int"      => [1, 2, 3, 4, 5],
    "array_mixed"    => [1, "two", 3.0, true, nothing],
)

function _normalize(v)
    # SurrealDB returns ints as Int64 and floats as Float64; numeric equality
    # already handles cross-type. Strings, bools, nothing pass through. Vectors
    # need recursive normalization for the array_mixed case.
    if v isa AbstractVector
        return Any[_normalize(x) for x in v]
    end
    return v
end

function run_interop_tests()
    db = SurrealDB.connect(URL; ns=NS, db=DB,
        auth=SurrealDB.RootAuth("root", "root"))

    rows = SurrealDB.select(db, "interop")
    @test rows isa AbstractVector
    if isempty(rows)
        @info "interop fixtures empty; skipping (run write_test_data.py first)"
        SurrealDB.close!(db)
        return
    end

    by_kind = Dict{String, Any}()
    for row in rows
        haskey(row, "kind") || continue
        by_kind[row["kind"]] = row
    end

    @testset "$(kind)" for (kind, expected) in EXPECTED
        @test haskey(by_kind, kind)
        if haskey(by_kind, kind)
            actual = _normalize(by_kind[kind]["value"])
            @test actual == expected
        end
    end

    # Nested object: spot-check leaf values rather than full-tree equality
    # (the datetime field round-trips as a String in JSON wire format, not
    # as a Julia DateTime, so byte-equality on the nested struct would
    # require a tz-aware comparison helper).
    if haskey(by_kind, "nested_object")
        nested = by_kind["nested_object"]["value"]
        @test nested isa AbstractDict
        @test haskey(nested, "outer")
        outer = nested["outer"]
        @test outer["inner"][1] == 10
        @test outer["inner"][2] == 20
        @test outer["inner"][3]["deep"] == "leaf"
        @test haskey(outer, "ts")
        # ts is an ISO-8601 string after round-trip; sanity-check format.
        @test occursin(r"^2024-01-15", string(outer["ts"]))
    end

    SurrealDB.close!(db)
end

@testset "interop: python writes, julia reads" begin
    run_interop_tests()
end
