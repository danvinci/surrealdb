@testset "Show methods" begin
    # Compact show output for human-readable REPL printing.
    r = SurrealDB.RecordID("user", "abc123")
    @test sprint(show, r) == "RecordID(\"user:abc123\")"

    t = SurrealDB.Table("stream")
    @test sprint(show, t) == "Table(\"stream\")"

    sub = SurrealDB.LiveSubscription("uuid-1234", Channel{Any}(1), nothing, true)
    @test sprint(show, sub) == "LiveSubscription(uuid-1234, active)"
    sub.active = false
    @test sprint(show, sub) == "LiveSubscription(uuid-1234, killed)"

    # `for n in sub` iterates the channel directly.
    sub2 = SurrealDB.LiveSubscription("uuid-5", Channel{Any}(8), nothing, true)
    put!(sub2.channel, Dict("action" => "CREATE", "id" => 1))
    put!(sub2.channel, Dict("action" => "UPDATE", "id" => 2))
    close(sub2.channel)
    actions = [n["action"] for n in sub2]
    @test actions == ["CREATE", "UPDATE"]

    # Auth structs MUST redact passwords — anywhere a client logs an auth
    # struct (debug printing, error messages, tracebacks) the password
    # cannot leak. JWT tokens get truncated rather than fully redacted so
    # algorithm/header inspection still works for debugging.
    root = SurrealDB.RootAuth("admin", "supersecret-password-do-not-leak")
    out = sprint(show, root)
    @test !occursin("supersecret", out)
    @test occursin("admin", out)
    @test occursin("***", out)

    ns = SurrealDB.NamespaceAuth("ns", "db", "user", "pw-leak-bad")
    @test !occursin("pw-leak-bad", sprint(show, ns))

    sc = SurrealDB.ScopedAuth("ns", "db", "account", "u", "p-leak-bad")
    @test !occursin("p-leak-bad", sprint(show, sc))

    short = SurrealDB.JwtAuth("short")
    @test sprint(show, short) == "JwtAuth(***)"

    long = SurrealDB.JwtAuth("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.signature")
    out2 = sprint(show, long)
    @test occursin("eyJhbGci", out2)         # algorithm prefix kept (8 chars)
    @test !occursin("signature", out2)       # secret-bearing tail dropped
end

@testset "RecordID" begin
    r = SurrealDB.RecordID("user", "abc123")
    @test r.table == "user"
    @test r.id == "abc123"
    @test string(r) == "user:abc123"

    r2 = SurrealDB.RecordID("user:abc123")
    @test r2.table == "user"
    @test r2.id == "abc123"

    # Integer id (regression: outer constructor used to self-recurse)
    r3 = SurrealDB.RecordID("user", 42)
    @test r3.table == "user"
    @test r3.id == 42
    @test string(r3) == "user:42"

    @test_throws ArgumentError SurrealDB.RecordID("no_colon")
end

@testset "Table" begin
    t = SurrealDB.Table("stream")
    @test t.name == "stream"
    @test string(t) == "stream"
end

@testset "SurrealValue" begin
    sv = SurrealDB.SurrealValue(SurrealDB.SR_INT, 42)
    @test sv.kind == SurrealDB.SR_INT
    @test sv.value == 42

    sv_none = SurrealDB.SurrealValue(SurrealDB.SR_NONE, nothing)
    @test sv_none.kind == SurrealDB.SR_NONE

    sv_str = SurrealDB.SurrealValue(SurrealDB.SR_STRING, "hello")
    @test sv_str.value == "hello"

    sv_bool = SurrealDB.SurrealValue(SurrealDB.SR_BOOL, true)
    @test sv_bool.value == true

    sv_dict = SurrealDB.SurrealValue(SurrealDB.SR_OBJECT, Dict("key" => 1))
    @test sv_dict.value["key"] == 1

    arr = [1, 2, 3]
    sv_arr = SurrealDB.SurrealValue(SurrealDB.SR_ARRAY, arr)
    @test sv_arr.value == arr
end

@testset "Auth types" begin
    ra = SurrealDB.RootAuth("root", "pass")
    @test ra.username == "root"
    @test ra.password == "pass"

    na = SurrealDB.NamespaceAuth("ns", "db", "user", "pass")
    @test na.namespace == "ns"
    @test na.database == "db"
    @test na.username == "user"
    @test na.password == "pass"

    sa = SurrealDB.ScopedAuth("ns", "db", "ac", "user", "pass")
    @test sa.namespace == "ns"
    @test sa.database == "db"
    @test sa.access == "ac"
    @test sa.username == "user"
    @test sa.password == "pass"

    ja = SurrealDB.JwtAuth("token123")
    @test ja.token == "token123"
end

@testset "Relationship" begin
    rel = SurrealDB.Relationship("person:john", SurrealDB.Table("knows"), "person:jane",
                                  Dict("met" => "2024-01-01"))
    @test rel.rel_in == "person:john"
    @test rel.relation.name == "knows"
    @test rel.rel_out == "person:jane"
    @test rel.data["met"] == "2024-01-01"

    rel2 = SurrealDB.Relationship(SurrealDB.RecordID("person", "john"),
                                   SurrealDB.Table("knows"),
                                   SurrealDB.RecordID("person", "jane"),
                                   Dict{String, Any}())
    @test string(rel2.rel_in) == "person:john"
    @test string(rel2.rel_out) == "person:jane"
    @test isempty(rel2.data)
end

@testset "LiveNotification" begin
    # v3 envelope shape (with session)
    n = SurrealDB.LiveNotification(Dict(
        "action" => "CREATE",
        "id" => "abc-123",
        "record" => "users:johnny",
        "result" => Dict("username" => "johnny"),
        "session" => "sess-1",
    ))
    @test n isa SurrealDB.LiveNotification
    @test n isa AbstractDict
    @test n.action == "CREATE"
    @test n.query_id == "abc-123"
    @test n.record == "users:johnny"
    @test n.result == Dict("username" => "johnny")
    @test n.session == "sess-1"

    # Dict-style access (backwards compat with raw-dict callers)
    @test n["action"] == "CREATE"
    @test n["id"] == "abc-123"
    @test n["record"] == "users:johnny"
    @test n["session"] == "sess-1"
    @test get(n, "action", "") == "CREATE"
    @test get(n, "missing", "DEFAULT") == "DEFAULT"
    @test haskey(n, "action")
    @test !haskey(n, "foo")
    @test length(n) == 5
    @test_throws KeyError n["foo"]

    # Iteration yields all five pairs
    pairs = collect(n)
    @test length(pairs) == 5
    @test ("action" => "CREATE") in pairs

    # v2 envelope (no session field)
    n2 = SurrealDB.LiveNotification(Dict(
        "action" => "UPDATE",
        "id" => "abc",
        "record" => "users:johnny",
        "result" => Dict("v" => 2),
    ))
    @test n2.session === nothing

    # Show
    @test occursin("CREATE", sprint(show, n))
    @test occursin("users:johnny", sprint(show, n))
end

@testset "Auth params conversion" begin
    params = SurrealDB._to_params(SurrealDB.RootAuth("r", "p"))
    @test params["user"] == "r"
    @test params["pass"] == "p"
    @test !haskey(params, "NS")

    params = SurrealDB._to_params(SurrealDB.NamespaceAuth("n", "d", "u", "p"))
    @test params["NS"] == "n"
    @test params["DB"] == "d"
    @test params["user"] == "u"
    @test params["pass"] == "p"

    params = SurrealDB._to_params(SurrealDB.ScopedAuth("n", "d", "a", "u", "p"))
    @test params["NS"] == "n"
    @test params["DB"] == "d"
    @test params["AC"] == "a"
    @test params["user"] == "u"
    @test params["pass"] == "p"
end
