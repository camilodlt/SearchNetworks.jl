module SearchNetworks
using DataFrames
using DuckDB

# Types
include("types.jl")
export Abstract_Table
export Abstract_Nodes
export Abstract_Edges

# TABLES 
include("sn_tables.jl")
export SN_col_type, create_DB, create_SN_tables!, get_nodes_from_db, get_edges_from_db
export write_to_nodes!, write_to_edges!
export write_only_new_to_nodes!

include("processing_tables.jl")
export load_multiple_dbs!
export combine_independent_dbs!

# Facilitators
include("sn_handy_functions.jl")
export get_table_by_type
export forbidden_cols_for_table

end
