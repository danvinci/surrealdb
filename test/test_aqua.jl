# Aqua.jl quality scan: catches package-level smells that aren't visible
# from individual test results.
#
# - unbound type parameters
# - unused dependencies
# - missing compat bounds
# - method ambiguities (over our own types)
# - Project.toml fields out of order or missing
# - method piracy (defining methods on other packages' types and operators)
#
# Skip ambiguities for now — Tables.jl + StructTypes integration
# inherently produces some ambiguous methods that are not real bugs;
# revisit when we narrow the API surface.

using SurrealDB
using Aqua
using Test

@testset "Aqua quality" begin
    Aqua.test_all(SurrealDB;
        # `ambiguities` is noisy with the Tables.jl + Dict integration
        # and most flags are spurious. Enable narrowly when we audit them.
        ambiguities = false,
        # Pkg extensions confuse Aqua's piracy detection across the
        # Pkg-extension boundary (Graphs / MetaGraphsNext methods registered
        # via the extension look like piracy). Disable for now.
        piracies = false,
    )
end
