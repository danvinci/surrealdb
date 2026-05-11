client = get_test_client()

# Bootstrap: create tables for live queries (v3 requires existing tables)
for tbl in ["test_live", "test_live_notif", "test_live_upd", "test_live_del", "test_live_a", "test_live_b", "test_live_dc"]
    println(stderr, "[bootstrap] create $tbl"); flush(stderr)
    try SurrealDB.create(client, tbl * ":__init", Dict("_init" => true)); catch; end
    println(stderr, "[bootstrap] done $tbl"); flush(stderr)
end

@testset "Live subscription creation" begin
    clean_table!(client, "test_live")
    sub = SurrealDB.live(client, "test_live")
    @test sub isa SurrealDB.LiveSubscription
    @test sub.active
    @test sub.channel isa Channel
    SurrealDB.kill!(sub)
    @test !sub.active
end

@testset "Live kill by id" begin
    clean_table!(client, "test_live")
    sub = SurrealDB.live(client, "test_live")
    SurrealDB.kill!(client, sub.query_id)
    @test !sub.active
end

@testset "Live with diff option" begin
    clean_table!(client, "test_live")
    sub = SurrealDB.live(client, "test_live"; diff=true)
    @test sub isa SurrealDB.LiveSubscription
    @test sub.active
    SurrealDB.kill!(sub)
    @test !sub.active
end

@testset "Live notification on create" begin
    clean_table!(client, "test_live_notif")
    sub = SurrealDB.live(client, "test_live_notif")
    @test sub.active
    @async begin
        sleep(0.3)
        SurrealDB.create(client, "test_live_notif", Dict("event" => "lived"))
    end
    notif = nothing
    for n in sub.channel
        notif = n
        break
    end
    @test notif !== nothing
    @test notif isa AbstractDict
    @test get(notif, "action", "") in ["CREATE", "UPDATE", "DELETE"]
    SurrealDB.kill!(sub)
    clean_table!(client, "test_live_notif")
end

@testset "Live notification on update" begin
    clean_table!(client, "test_live_upd")
    SurrealDB.create(client, "test_live_upd:watch", Dict("val" => 1))
    sub = SurrealDB.live(client, "test_live_upd")
    @async begin
        sleep(0.3)
        SurrealDB.update(client, "test_live_upd:watch", Dict("val" => 2))
    end
    notif = nothing
    for n in sub.channel
        notif = n
        break
    end
    @test notif !== nothing
    @test notif isa AbstractDict
    SurrealDB.kill!(sub)
    clean_table!(client, "test_live_upd")
end

@testset "Live notification on delete" begin
    clean_table!(client, "test_live_del")
    SurrealDB.create(client, "test_live_del:bye", Dict("x" => 1))
    sub = SurrealDB.live(client, "test_live_del")
    @async begin
        sleep(0.3)
        SurrealDB.delete(client, "test_live_del:bye")
    end
    notif = nothing
    for n in sub.channel
        notif = n
        break
    end
    @test notif !== nothing
    @test notif isa AbstractDict
    SurrealDB.kill!(sub)
    clean_table!(client, "test_live_del")
end

@testset "Live multiple tables" begin
    clean_table!(client, "test_live_a")
    clean_table!(client, "test_live_b")
    sub_a = SurrealDB.live(client, "test_live_a")
    sub_b = SurrealDB.live(client, "test_live_b")
    @test sub_a.query_id != sub_b.query_id
    @test sub_a.active
    @test sub_b.active
    SurrealDB.kill!(sub_a)
    SurrealDB.kill!(sub_b)
end

@testset "Live disconnect closes channel" begin
    clean_table!(client, "test_live_dc")
    sub = SurrealDB.live(client, "test_live_dc")
    @test sub.active
    SurrealDB.kill!(sub)
    @test !sub.active
    @test !isopen(sub.channel)
end

SurrealDB.close!(client)
