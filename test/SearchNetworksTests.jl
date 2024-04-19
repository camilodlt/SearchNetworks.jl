module SearchNetworksTests
using DuckDB
using DBInterface
using SearchNetworks
using ReTest
# --- MAGE LOGGER ---

# DUCK DB TABLE OF CONNECTIONS & TABLE OF IDS 

# include("tables_test.jl")
@testset "DB CREATION, Writing" begin
    include("tables_test.jl")
end

# --- PRE PROCESSING ---
@testset "DB Combination" begin
    include("processing_db_test.jl")
end



# --- VISUALIZATION --- 

end
