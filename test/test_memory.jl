# Memory-leak smoke test for the embedded path.
#
# Catches Julia-side allocator leaks: iterators, dicts, channel handles, or
# RecordID objects that don't get GC'd between iterations. Does NOT catch
# C-side leaks in `libsurreal` (the FFI layer mallocs sr_value_t buffers
# that are freed via `sr_value_free` — leaks there need valgrind, out of
# scope for an in-process SDK test).
#
# Strategy: warm up, measure heap, run 10x more ops, measure again. Assert
# the post-scale heap is within a generous slack of post-warmup. The test
# is intentionally loose — we're guarding against unbounded growth (a
# real leak makes RSS climb linearly with iterations), not micro-allocs.
#
# Embedded-only; requires libsurreal loaded.

using SurrealDB
using Test

const LEAK_TABLE = "leak_check"

function _ops_loop(db, n::Int)
    for i in 1:n
        rec = SurrealDB.RecordID(LEAK_TABLE, "k$i")
        SurrealDB.create(db, rec, Dict("v" => i))
        SurrealDB.select(db, rec)
        SurrealDB.update(db, rec, Dict("v" => i * 2))
        SurrealDB.delete(db, rec)
    end
end

function _heap_after(f, gc_count::Int=2)
    for _ in 1:gc_count
        GC.gc(true)
    end
    f()
    for _ in 1:gc_count
        GC.gc(true)
    end
    return Base.gc_live_bytes()
end

@testset "embedded path: heap doesn't grow unboundedly under sustained load" begin
    db = SurrealDB.connect("mem://")
    try
        SurrealDB.use!(db, "test", "test")
        # Warm-up: precompile the CRUD methods, populate caches.
        _ops_loop(db, 100)

        # Baseline heap after warmup.
        baseline = _heap_after(() -> _ops_loop(db, 1_000))

        # Scale up by 10x. A linear leak would make heap grow ~10x; we
        # tolerate up to 4x slack to absorb GC noise + FFI buffer
        # variance + cache fill.
        scaled = _heap_after(() -> _ops_loop(db, 10_000))

        ratio = scaled / baseline
        @info "heap ratio (10x ops)" baseline_bytes=baseline scaled_bytes=scaled ratio=ratio
        @test ratio < 4.0
    finally
        try; SurrealDB.close!(db); catch; end
    end
end
