# Public API stability snapshot.
#
# Asserts the exported symbol list matches a frozen baseline. Any addition
# or removal triggers a test failure: the fix is to update EXPECTED here in
# the same commit that changes the public surface, making the change show
# up in code review.
#
# This catches accidental API churn (auto-export from `using` an internal
# symbol that grew an `export` line, removal of a symbol someone external
# might depend on) without prescribing what the API should be.

using SurrealDB
using Test

const EXPECTED_EXPORTS = Set([
    # Module
    :SurrealDB,

    # Core types
    :AbstractConnection, :RemoteConnection, :EmbeddedConnection,
    :SurrealClient, :SurrealSession,
    :RecordID, :Table, :SurrealValue, :Relationship, :LiveSubscription,

    # Auth
    :RootAuth, :NamespaceAuth, :ScopedAuth, :JwtAuth,

    # Connection lifecycle
    :connect, :close!, :status, :events,

    # Auth methods
    :signin!, :signup!, :authenticate!, :invalidate!,

    # Database scope
    :use!, :info, :version, :health,

    # Query / CRUD
    :query, :query_table, :query_one,
    :create, :select, :update, :delete, :insert, :upsert, :merge,
    :relate, :insert_relation,
    :patch, :patch_add, :patch_remove, :patch_replace,
    :let!, :unset!,

    # Live queries
    :live, :kill!,

    # Transactions
    :begin!, :commit!, :cancel!,

    # Sessions
    :attach!, :detach!, :sessions,

    # Import / export
    :export_db, :import_db,

    # Embedded
    :libsurreal_load!,

    # Tables / extensions
    :to_table, :to_metagraph,

    # Errors
    :SurrealDBError, :SurrealError, :RPCError, :ConnectionError,
    :ServerError, :QueryError, :ValidationError, :ConfigurationError,
    :ThrownError, :SerializationError, :NotAllowedError, :NotFoundError,
    :AlreadyExistsError, :InternalError,
    :EmbeddedFFIError, :ConnectionUnavailableError, :UnsupportedEngineError,
    :UnsupportedFeatureError, :UnexpectedResponseError,
])

@testset "public API surface is stable" begin
    actual = Set(names(SurrealDB))
    added = setdiff(actual, EXPECTED_EXPORTS)
    removed = setdiff(EXPECTED_EXPORTS, actual)

    if !isempty(added)
        @info "new exports detected — update EXPECTED_EXPORTS or remove the export" added
    end
    if !isempty(removed)
        @info "exports removed — update EXPECTED_EXPORTS or restore the export" removed
    end

    @test isempty(added)
    @test isempty(removed)
    @test actual == EXPECTED_EXPORTS
end
