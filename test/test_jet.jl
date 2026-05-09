# JET.jl static type-stability check.
#
# JET reports calls where a type cannot be inferred (returning `Any` past a
# point where it shouldn't), method-error candidates (typos, missing methods),
# and other inference-time anomalies.
#
# We don't enforce zero reports today — Tables.jl + Dict-of-Any pervade the
# codebase and produce inherently dynamic dispatches. We DO smoke-test that
# JET successfully runs on the package without crashing, and surface a count
# so a sudden spike in dynamic dispatches is visible in CI logs.
#
# When JET stabilizes around a fixed allowlist, this can become a hard
# regression gate (`@test isempty(JET.get_reports(...))`).

using SurrealDB
using JET
using Test

@testset "JET package report (advisory)" begin
    # report_package walks the entire package; uses the
    # `concretization_patterns` default which expands constants.
    result = JET.report_package(SurrealDB; toplevel_logger=nothing)
    n = length(JET.get_reports(result))
    @info "JET reports" count=n
    # Smoke: JET ran without erroring out. The count check is intentionally
    # generous; tighten when the codebase is more type-stable.
    @test n < 200
end
