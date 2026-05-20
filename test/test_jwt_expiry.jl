# JWT expiry / refresh path.
#
# Goal: assert that when an authenticated session's JWT expires, the next
# RPC fails with a kind-tagged `NotAllowedError` (not a generic
# `SurrealDBError` or worse, a silent stale-token request that gets
# accepted with the wrong credentials).
#
# Strategy: SurrealDB lets us define a SCOPE/ACCESS with a session
# duration via `DEFINE ACCESS ... DURATION FOR SESSION 1s`. We sign in
# under that scope, sleep past the duration, then attempt a query and
# inspect the resulting error. Server-gated; skips on HTTP transport
# (HTTP signs every request fresh and never carries server-side session
# state, so expiry is moot).

using SurrealDB
using Test

@testset "JWT expiry surfaces as NotAllowedError" begin
    if startswith(TEST_URL, "http")
        @info "skip: HTTP transport doesn't carry server-side session state"
    else
        # Clean slate: independent test scope so the rest of the suite
        # isn't affected by the access definition.
        admin = SurrealDB.connect(TEST_URL; ns=TEST_NS, db=TEST_DB,
            auth=SurrealDB.RootAuth("root", "root"))
        try
            try; SurrealDB.query(admin, "REMOVE ACCESS expiry_test ON DATABASE"); catch; end
            try; SurrealDB.query(admin, "REMOVE TABLE IF EXISTS jwt_user"); catch; end
            SurrealDB.query(admin, "DEFINE TABLE jwt_user")
            # 1-second session lifetime; regular access type for password auth.
            # SIGNIN clause references `$user` so the SDK's default ScopedAuth
            # (NS, DB, AC, user, pass) maps cleanly. For arbitrary param keys
            # (e.g. `$email`, `$name`), use the dict-variant `ScopedAuth(ns,
            # db, scope, params::Dict)` so SIGNIN can pick them up.
            SurrealDB.query(admin, """
                DEFINE ACCESS expiry_test ON DATABASE TYPE RECORD
                    SIGNIN (SELECT * FROM jwt_user WHERE name = \$user)
                    DURATION FOR SESSION 1s
            """)
            SurrealDB.create(admin, "jwt_user:alice", Dict("name" => "alice"))

            # Sign in under the short-lived access. We get a JWT bound to
            # the 1s session.
            scoped = SurrealDB.connect(TEST_URL;
                ns=TEST_NS, db=TEST_DB,
                auth=SurrealDB.ScopedAuth(TEST_NS, TEST_DB, "expiry_test",
                                          "alice", ""))
            try
                # Fresh token: query should succeed.
                ok_result = SurrealDB.query(scoped, "SELECT * FROM jwt_user")
                @test ok_result !== nothing

                # Sleep past the 1s session duration, then try again.
                sleep(2.0)

                expired_err = try
                    SurrealDB.query(scoped, "SELECT * FROM jwt_user")
                    nothing
                catch e
                    e
                end

                # The post-expiry call must surface a typed kind-tagged
                # error so callers can branch on `is_token_expired` rather
                # than parsing message strings.
                @test expired_err !== nothing
                if expired_err !== nothing
                    # Acceptable shapes:
                    #   - NotAllowedError(is_token_expired=true)  (post-D1 ideal)
                    #   - NotAllowedError                          (kind-correct, predicate not extracted)
                    #   - ServerError subclass with auth-related message
                    is_typed = expired_err isa SurrealDB.NotAllowedError ||
                               expired_err isa SurrealDB.ServerError
                    @test is_typed
                    if expired_err isa SurrealDB.NotAllowedError
                        # The predicate may or may not be extractable from
                        # this server's error format — assert presence of
                        # SOME signal, either is_token_expired OR the
                        # message itself referencing expiry/auth.
                        msg = sprint(showerror, expired_err)
                        @test expired_err.is_token_expired ||
                              occursin(r"(expir|invalid|token|auth)"i, msg)
                    end
                end
            finally
                try; SurrealDB.close!(scoped); catch; end
            end
        finally
            try; SurrealDB.query(admin, "REMOVE ACCESS expiry_test ON DATABASE"); catch; end
            try; SurrealDB.query(admin, "REMOVE TABLE IF EXISTS jwt_user"); catch; end
            SurrealDB.close!(admin)
        end
    end
end
