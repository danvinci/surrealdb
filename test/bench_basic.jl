# Basic benchmarks for SurrealDB.jl. Compares against Go SDK reference
# numbers (target: within 2x).
#
# Usage:
#   SURREALDB_URL=ws://localhost:8000 julia --project=. test/bench_basic.jl
#
# Pass `--json` to emit machine-readable output for CI consumption.

using SurrealDB
using Printf
using JSON

const TEST_URL = get(ENV, "SURREALDB_URL", "ws://localhost:8000")
const ITERATIONS = parse(Int, get(ENV, "BENCH_ITER", "1000"))
const WARMUP = 10

# Reference numbers from the Go SDK on localhost (ms/op). The CI regression
# check fails if any benchmark exceeds 2x the reference.
const GO_REFERENCE_MS = Dict(
    "select_const" => 1.5,
    "create_select" => 3.0,
    "select_table" => 2.0,
)

function bench_select_const(client)
    t = @elapsed for _ in 1:ITERATIONS
        SurrealDB.query(client, "SELECT * FROM 1")
    end
    return t / ITERATIONS * 1000
end

function bench_create_select(client)
    t = @elapsed for i in 1:ITERATIONS
        rid = SurrealDB.RecordID("bench", "bs$i")
        SurrealDB.create(client, rid, Dict("val" => i))
        SurrealDB.select(client, rid)
    end
    return t / ITERATIONS * 1000
end

function bench_select_table(client)
    t = @elapsed for _ in 1:ITERATIONS
        SurrealDB.select(client, "bench")
    end
    return t / ITERATIONS * 1000
end

function run_benchmarks(json_output::Bool)
    client = SurrealDB.connect(TEST_URL;
        ns="test", db="test",
        auth=SurrealDB.RootAuth("root", "root"))

    # Idempotent setup: tolerate the table or pre-records already existing
    # (e.g. CI runs the bench twice in one job for human + JSON output).
    try; SurrealDB.query(client, "REMOVE TABLE IF EXISTS bench"); catch; end
    SurrealDB.query(client, "DEFINE TABLE bench TYPE ANY SCHEMALESS")
    for i in 1:100
        try
            SurrealDB.create(client, SurrealDB.RecordID("bench", "pre$i"),
                Dict("val" => i))
        catch e
            e isa SurrealDB.AlreadyExistsError || rethrow()
        end
    end

    for _ in 1:WARMUP
        SurrealDB.query(client, "SELECT * FROM 1")
    end

    results = [
        ("select_const",  bench_select_const(client)),
        ("create_select", bench_create_select(client)),
        ("select_table",  bench_select_table(client)),
    ]

    SurrealDB.query(client, "DELETE FROM bench")
    SurrealDB.close!(client)

    if json_output
        # Format compatible with github-action-benchmark's "customSmallerIsBetter".
        json_results = [
            Dict("name" => name,
                 "unit" => "ms/op",
                 "value" => round(ms; digits=4),
                 "extra" => "Go reference: $(GO_REFERENCE_MS[name]) ms/op")
            for (name, ms) in results
        ]
        println(JSON.json(json_results))
    else
        println("SurrealDB.jl Benchmark")
        println("="^50)
        println("URL: $TEST_URL")
        println("Iterations: $ITERATIONS (warmup: $WARMUP)")
        println()
        for (name, ms) in results
            ref = GO_REFERENCE_MS[name]
            ratio = ms / ref
            marker = ratio > 2.0 ? " ⚠ regression" :
                     ratio > 1.5 ? " (slow)" : ""
            @printf "%-16s %.3f ms/op   (Go ref: %.1f, ratio %.2fx)%s\n" name ms ref ratio marker
        end
        println()
        println("Target: within 2x of Go reference for each benchmark.")
    end

    # Exit non-zero if any benchmark exceeds 2x the Go reference, so CI
    # surfaces regressions. Threshold is conservative — Julia should be
    # competitive on raw RPC throughput.
    regressed = [name for (name, ms) in results if ms > 2.0 * GO_REFERENCE_MS[name]]
    if !isempty(regressed)
        @error "benchmarks exceeded 2x Go reference" failed=regressed
        exit(1)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    json_output = "--json" in ARGS
    run_benchmarks(json_output)
end
