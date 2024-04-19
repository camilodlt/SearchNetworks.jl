# LIST MANDATORY COLS --- ---  

"""
    forbidden_cols_for_table(::Abstract_Nodes)

Returns the mandatory cols in Nodes according to the SN documentation
"""
function forbidden_cols_for_table(::Type{Abstract_Nodes})
    return ["id_hash"]
end

"""
    forbidden_cols_for_table(::Abstract_Edges)

Returns the mandatory cols in Edges according to the SN documentation
"""
function forbidden_cols_for_table(::Type{Abstract_Edges})
    return ["_from", "_to", "iteration"]
end

"""
    forbidden_cols_for_table(::Abstract_Table)

Not implemented
"""
function forbidden_cols_for_table(::Type{<:Abstract_Table})
    throw(AssertionError("Not implemented"))
end

# GET A TABLE BY TYPE --- ---  

"""
    get_table_by_type(::Type{<:Abstract_Table})

Get the nodes table
"""
function get_table_by_type(con::DuckDB.DB, ::Type{Abstract_Nodes})
    return get_nodes_from_db(con)
end

"""
    get_table_by_type(::Type{<:Abstract_Table})

Get the edges table
"""
function get_table_by_type(con::DuckDB.DB, ::Type{Abstract_Edges})
    return get_edges_from_db(con)
end
