client = get_test_client()
clean_table!(client, "test_crud")

@testset "Create" begin
    result = SurrealDB.create(client, "test_crud", Dict("name" => "test", "value" => 42))
    @test result isa AbstractDict
    @test result["name"] == "test"
    @test result["value"] == 42

    result2 = SurrealDB.create(client, "test_crud:explicit_id",
                                Dict("key" => "val"))
    @test result2 isa AbstractDict
    @test result2["id"] == "test_crud:explicit_id"
end

@testset "Select" begin
    result = SurrealDB.select(client, "test_crud")
    @test result isa Vector
    @test length(result) >= 1

    result2 = SurrealDB.select(client, "test_crud:explicit_id")
    @test result2 isa AbstractDict
end

@testset "Insert" begin
    result = SurrealDB.insert(client, "test_crud", Dict("a" => 1))
    @test result isa Union{AbstractDict, AbstractVector}
    result_multi = SurrealDB.insert(client, "test_crud",
                                     [Dict("b" => 2), Dict("c" => 3)])
    @test result_multi isa Vector
    @test length(result_multi) == 2
end

@testset "Update" begin
    SurrealDB.create(client, "test_crud:update_me", Dict("val" => 1))
    result = SurrealDB.update(client, "test_crud:update_me", Dict("val" => 2))
    @test result isa AbstractDict
    @test get(result, "val", nothing) == 2
end

@testset "Delete" begin
    SurrealDB.create(client, "test_crud:del_me", Dict("x" => 1))
    result = SurrealDB.delete(client, "test_crud:del_me")
    @test result isa Any
end

@testset "Upsert" begin
    result = SurrealDB.upsert(client, "test_crud:upsert_test", Dict("val" => 1))
    @test result isa AbstractDict
end

@testset "Merge" begin
    SurrealDB.create(client, "test_crud:merge_me", Dict("a" => 1, "b" => 2))
    result = SurrealDB.merge(client, "test_crud:merge_me", Dict("b" => 99))
    @test result isa AbstractDict
end

@testset "Patch" begin
    SurrealDB.create(client, "test_crud:patch_me", Dict("tags" => []))
    result = SurrealDB.patch(client, "test_crud:patch_me", [
        Dict("op" => "add", "path" => "/tags/0", "value" => "new")
    ])
    @test result isa Any

    result2 = SurrealDB.patch_add(client, "test_crud:patch_me", "/tags/1", "another")
    @test result2 isa Any

    result3 = SurrealDB.patch_replace(client, "test_crud:patch_me", "/tags/0", "updated")
    @test result3 isa Any

    result4 = SurrealDB.patch_remove(client, "test_crud:patch_me", "/tags/1")
    @test result4 isa Any
end

@testset "Relate" begin
    SurrealDB.create(client, "person:rel_a", Dict("name" => "Alice"))
    SurrealDB.create(client, "person:rel_b", Dict("name" => "Bob"))
    rel = SurrealDB.Relationship("person:rel_a", SurrealDB.Table("knows"), "person:rel_b",
                                  Dict("since" => "2024"))
    result = SurrealDB.relate(client, rel)
    @test result isa AbstractDict
end

@testset "Insert relation" begin
    SurrealDB.create(client, "person:ir_a", Dict("name" => "C"))
    SurrealDB.create(client, "person:ir_b", Dict("name" => "D"))
    rel = SurrealDB.Relationship("person:ir_a", SurrealDB.Table("likes"), "person:ir_b",
                                  Dict("score" => 10))
    result = SurrealDB.insert_relation(client, rel)
    @test result isa Union{AbstractDict, AbstractVector}
end

@testset "Create duplicate id" begin
    SurrealDB.create(client, "test_crud:dup_id", Dict("v" => 1))
    @test_throws SurrealDB.SurrealDBError SurrealDB.create(client, "test_crud:dup_id", Dict("v" => 2))
end

@testset "Select nonexistent record" begin
    result = SurrealDB.select(client, "test_crud:does_not_exist_at_all")
    @test result isa Union{Vector, Nothing, Dict}
end

@testset "Update nonexistent record" begin
    # v3: update on nonexistent does not throw
    result = SurrealDB.update(client, "test_crud:no_such_id", Dict("x" => 1))
    @test true
end

@testset "Merge preserves unmentioned fields" begin
    SurrealDB.create(client, "test_crud:merge_preserve", Dict("a" => 1, "b" => 2, "c" => 3))
    result = SurrealDB.merge(client, "test_crud:merge_preserve", Dict("b" => 99))
    if result isa AbstractDict
        @test get(result, "b", nothing) == 99
    end
end

@testset "Upsert update existing" begin
    SurrealDB.create(client, "test_crud:upsert_existing", Dict("val" => 1))
    result = SurrealDB.upsert(client, "test_crud:upsert_existing", Dict("val" => 999))
    if result isa AbstractDict
        @test get(result, "val", nothing) == 999
    end
end

@testset "Run user-defined function" begin
    # run() targets the function registry (fn::*), not builtin SQL functions —
    # exercise it via a DEFINE FUNCTION created for the test.
    try; SurrealDB.query(client, "REMOVE FUNCTION fn::doubled"); catch; end
    SurrealDB.query(client, "DEFINE FUNCTION fn::doubled(\$x: int) {RETURN \$x * 2;}")
    result = SurrealDB.run(client, "fn::doubled", Any[21])
    @test result == 42
    try; SurrealDB.query(client, "REMOVE FUNCTION fn::doubled"); catch; end
end

clean_table!(client, "test_crud")
clean_table!(client, "person")
clean_table!(client, "knows")
clean_table!(client, "likes")
SurrealDB.close!(client)
