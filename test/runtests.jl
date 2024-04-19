using Test
using DuckDB
using DBInterface
using SearchNetworks
using DataFrames

@testset "DB CREATION, Writing" begin
    include("tables_test.jl")
    include("processing_db_test.jl")
end
