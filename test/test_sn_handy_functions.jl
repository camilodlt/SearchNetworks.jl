@testset "Handy Functions" begin
    @test begin # mandatory cols in nodes
        n = Abstract_Nodes
        _forbidden_cols_per_table(n) == ["id_hash"]
    end
    @test begin # mandatory cols in edges
        n = Abstract_Edges
        _forbidden_cols_per_table(n) == ["_from", "_to", "iteration"]
    end
end
