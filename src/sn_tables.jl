
####################################################
# CREATE SN TABLES                                 #
####################################################

using DuckDB
import DataStructures: OrderedDict
import DataFrames: DataFrame
using Logging

SEQ_AUTO_INCREMENT_NAME = "edges_id_auto_increment" # Not used anymore, but was to make a PK if needed

"""
    _execute_command(con::DuckDB.DB, command::String)

Just executes the command and return the QueryResult
"""
function _execute_command(con::DuckDB.DB, command::String)
    qr = DBInterface.execute(con, command)
    return qr
end

# COL TYPES (basic abstraction) --- 
abstract type ColumnType end

"""
SN_col_type 

It helps to map from julia types to DuckDB types. 
Like string => VARCHAR
"""
struct SN_col_type <: ColumnType
    string::Bool
    float::Bool
end

"""
    SN_col_type(; string::Bool = false, float::Bool = false)

The col can be of one (and only one) type (otherwise an AssertionError is raised.)
"""
function SN_col_type(; string::Bool = false, float::Bool = false)
    if !string && !float
        throw(AssertionError("All types are false. One has to be true"))
    end
    if string && float
        throw(AssertionError("All types are true, only one can be true"))
    end
    return SN_col_type(string, float)
end

"""
    getDBtype(sn_col::SN_col_type)

Maps the julia type from `sn_col` to the type in DuckDB in the DuckDB type as a string.
"""
function getDBtype(sn_col::SN_col_type)
    if sn_col.string
        return "VARCHAR"
    end
    if sn_col.float
        return "DOUBLE"
    end
end


# Extract Extra cols --- 
"""
    _extract_extra_cols_info(extra_cols::Dict{String,ColumnType})

Separates cols from types (keys from values).
Checks that the column name is not forbidden (because reserved).

The reserved keywords are : ["_from", "_to", "iteration", "id_hash"]

An `AssertionError` is raised a column name is not accepted. 

Returns cols and types: 
    - Cols : Vector{String}
    - types : Vector{<:ColumnType}
"""
function _extract_extra_cols_info(extra_cols::OrderedDict{String,<:ColumnType})
    col_names = [i[1] for i in extra_cols]
    types = [i[2] for i in extra_cols]
    forbidden_cols = ["edge_id", "_from", "_to", "iteration", "id_hash"]
    bad_cols = intersect(Set(col_names), Set(forbidden_cols))
    if length(bad_cols) > 0
        throw(
            AssertionError("Not accepted columns because they are reserved : $(bad_cols)"),
        )
    end
    col_names, types
end

"""
    _convert_types_to_DB_format(types::Vector{<:ColumnType})

Broadcasts `getDBtype` to `types`
"""
function _convert_types_to_DB_format(types::Vector{<:ColumnType})
    return getDBtype.(types)
end

"""

    _create_extra_cols_suffix(cols::Vector{String}, types::Vector{String})

Combines cols and types in a single String line. Intended to be used 
in an SQL statement, after the mandatory cols. 

cols : ["f", "t"]
types : ["VARCHAR","VARCHAR"]

output : f VARCHAR, t VARCHAR
"""
function _create_extra_cols_suffix(cols::Vector{String}, types::Vector{String})
    @assert length(cols) == length(types)
    cols = ["$(b[1]) $(b[2])" for b in zip(cols, types)]
    return join(cols, ", ")
end

"""
    _create_command_extra_cols(extra_cols::OrderedDict{String,<:ColumnType})

From a mapping {name => SN_col_type, ...} creates the string that is to be 
appended to the CREATE TABLE sql command.  
"""
function _create_command_extra_cols(extra_cols::OrderedDict{String,<:ColumnType})
    # Get the new cols to create
    cols_extra, types_extra = _extract_extra_cols_info(extra_cols)
    types_extra = _convert_types_to_DB_format(types_extra)
    # Get the command
    extra_command = _create_extra_cols_suffix(cols_extra, types_extra)
    return extra_command
end

"""
    _create_command_extra_cols(::Nothingr

Returns and empty string
"""
function _create_command_extra_cols(::Nothing)
    return ""
end

# Write the sql commands for table creation #
"""
    _create_nodes_command(extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing})

Creates the NODES table sql query.
The mandatory col is `id_hash`, its PK.
"""
function _create_nodes_command(extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing})
    extra_command = _create_command_extra_cols(extra_cols)
    command = "CREATE TABLE NODES (id_hash VARCHAR PRIMARY KEY, $(extra_command))"
    return command
end

"""
    _create_edges_command(extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing})

Creates the EDGES table sql query.
The mandatory col are : 
- `_from` FK linked to Nodes(id_hash).
- `_to` FK linked to Nodes(id_hash).
- `iteration` 

"""
function _create_edges_command(extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing})
    extra_command = _create_command_extra_cols(extra_cols)
    # if pk 
    # command = "CREATE TABLE EDGES (edge_id INTEGER PRIMARY KEY DEFAULT nextval('$SEQ_AUTO_INCREMENT_NAME'), _from VARCHAR REFERENCES NODES(id_hash), _to VARCHAR REFERENCES NODES(id_hash), iteration INTEGER, $(extra_command))"
    # else 
    command = "CREATE TABLE EDGES (_from VARCHAR REFERENCES NODES(id_hash), _to VARCHAR REFERENCES NODES(id_hash), iteration INTEGER, $(extra_command))"
    #end
    return command
end

# Create auto increment sequence for edges #
"""
    _create_auto_increment_seq_edges(con::DuckDB.DB)

Creates the query to create an sql Sequence. 
This has to be ran before the creation of the EDGES table.
"""
function _create_auto_increment_seq_edges(con::DuckDB.DB)
    command = "CREATE SEQUENCE $SEQ_AUTO_INCREMENT_NAME START 1;"
    _execute_command(con, command)
end

# Create Node Table --- 
"""
    _create_node_table(con::DuckDB.DB, extra_cols::Dict{String,ColumnType})

Makes the NODES query and executes it.
"""
function _create_nodes_table(
    con::DuckDB.DB,
    extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing},
)
    command = _create_nodes_command(extra_cols)
    _execute_command(con, command)
end
# Create Node Table --- 
"""
    _create_edges_table(con::DuckDB.DB, extra_cols::Dict{String,ColumnType})

Makes the EDGES query and executes it.
"""
function _create_edges_table(
    con::DuckDB.DB,
    extra_cols::Union{OrderedDict{String,<:ColumnType},Nothing},
)
    command = _create_edges_command(extra_cols)
    _execute_command(con, command)
end

"""
    _new_db(filename::Union{String,Nothing})

Creates a DB in memory or with the given filename.
"""
function _new_db(filename::Union{String,Nothing})
    return isnothing(filename) ? DuckDB.DB() : DuckDB.DB(filename)
end

# CREATE DB --- 
"""
    create_DB(filename::String)

Creates a persistent DB.
"""
function create_DB(filename::String)
    return _new_db(filename)
end

"""
    create_DB()

Creates an in memory DB.
"""
function create_DB()
    return _new_db(nothing)
end

# CREATE TABLES --- 
"""
    create_SN_tables!(
        db::DuckDB.DB;
        extra_nodes_cols::Union{OrderedDict{String,<:ColumnType},Nothing} = nothing,
        extra_edges_cols::Union{OrderedDict{String,<:ColumnType},Nothing} = nothing,
    )

Create SN tables. 
It first creates a auto increment sequence. Then the NODES table, finally the EDGES table.
The autoincrement sequence is the unique id in the EDGES table.

Tables : 
    - NODES 
    - EDGES

There is no return.
"""
function create_SN_tables!(
    db::DuckDB.DB;
    extra_nodes_cols::Union{OrderedDict{String,<:ColumnType},Nothing} = nothing,
    extra_edges_cols::Union{OrderedDict{String,<:ColumnType},Nothing} = nothing,
)
    _create_auto_increment_seq_edges(db)
    _create_nodes_table(db, extra_nodes_cols)
    _create_edges_table(db, extra_edges_cols)
    nothing
    # TODO V3, check that there is no intersection between extra cols in nodes and extra cols in edges
end

# TABLES TO DF --- 
"""
    get_nodes_from_db(con::DuckDB.DB)

Exectutes a `Select *` from the NODES table. 
Returns a DataFrame.
"""
function get_nodes_from_db(con::DuckDB.DB)
    # using the execute API and not the duckdb_query API which is not available in DuckDB 0.10.0 
    # query the database
    results = DBInterface.execute(con, "SELECT * from NODES")
    return DataFrame(results)
end


"""
    get_edges_from_db(con::DuckDB.DB)

Exectutes a `Select *` from the EDGES table. 
Returns a DataFrame.
"""
function get_edges_from_db(con::DuckDB.DB)
    # using the execute API and not the duckdb_query API which is not available in DuckDB 0.10.0 
    # query the database
    results = DBInterface.execute(con, "SELECT * from EDGES")
    return DataFrame(results)
end

# WRITE TO NODES #

# Note: Writes to the same table should be done by only one thread at a time.
# DuckDB uses multi version concurrency control so the writers will have the last valid 
# copy of tables, when a table is updated (at the end of the transaction), that view should too.
# The problemn is that between a read to verify that an id is not present and the succequent write, maybe another 
# thread already wrote with that id. 

# The problem with Optimistic concurrency control is that the first thread wins the race. 

# RULES for writing are as follows: 

# Nodes : We suppose that only one thread writes to a table at the same time for the NODES table. 
# It'll check if the hashes are not already present and then write the whole pack. 

# Edges without PK: Edges can be written concurrently because complete duplicates can exist as in Neutral Networks
# It is best to split the range of edges to write and spawn to different threads. 
# Nodes have to be written before edges

function _manual_write_close_appender(appender::DuckDB.Appender)
    # if appender.handle != C_NULL
    #     # flush and close
    #     ret = DuckDB.duckdb_appender_close(appender.handle)
    # end
    # appender.handle = C_NULL
    ret = 1
    if appender.handle != C_NULL
        # flush and close
        ret = DuckDB.duckdb_appender_destroy(appender.handle)
    end
    appender.handle = C_NULL
    return ret # 0 succes, 1 failure
end

# TODO V2 => HAVE DYNAMIC STRUCTS THAT VALIDATE THE DATA :)

"""
    function _append_content_to_appender(
        appender::DuckDB.Appender,
        content::Vector{Dict{String,Any}},
    )

Appends all elements in `content` to the `appender`. 
At the end it flushes the appender.
"""
function _append_content_to_appender(
    appender::DuckDB.Appender,
    content::Vector{OrderedDict{String,V}},
) where {V}
    for row in content
        for cell in row
            DuckDB.append(appender, cell[2])
        end
        DuckDB.end_row(appender)
    end
    # flush the appender after all rows
    # DuckDB.close(appender) # should call appender destroy which is recommended by DuckDB
end

"""
    write_to_nodes(con::DuckDB.DB, content::Vector{Dict{String,Any}})

Uses the Appender API to insert rows into the NODES table.
 
Every value is inserted one by one in the Appender, as such, all data is expected to be present in each element of `content`.

See : `_append_content_to_appender`
"""
function write_to_nodes!(con::DuckDB.DB, content::Vector{OrderedDict{String,V}}) where {V}
    # append data by row
    appender = DuckDB.Appender(con, "NODES")
    _append_content_to_appender(appender, content)
    result = _manual_write_close_appender(appender)
    # result = DBInterface.close!(appender)
    return result
end

"""
    write_to_nodes!(con::DuckDB.DB, content::Dict{String,Any})

Calls `write_to_nodes!` with a vector of length 1 composed only of the dict `content`.
"""
function write_to_nodes!(con::DuckDB.DB, content::OrderedDict{String,V}) where {V}
    write_to_nodes!(con, [content])
end


"""
    _get_ids_from_content(content::OrderedDict{String,V}) where {V}

Get's the "id_hash" for every row in the `content`.
"""
function _get_ids_from_content(content::Vector{OrderedDict{String,V}}) where {V}
    ids = [row["id_hash"] for row in content]
    return ids
end

"""
    _get_unique_from_content(content::Vector{OrderedDict{String,V}}) where {V}

Return the first ocurrences of any row in `content`.
Two rows are duplicates if they have the same id_hash. 

Example, content is a vector of : 
    - {"id_hash" = "1", "it" = 1}
    - {"id_hash" = "3", "it" = 2}
    - {"id_hash" = "1", "it" = 3}

This function will return the vector with : 
    - {"id_hash" = "1", "it" = 1}
    - {"id_hash" = "3", "it" = 2}

"""
function _get_unique_from_content(content::Vector{OrderedDict{String,V}}) where {V}

    # get the unique rows (first occurence ) 
    unique_indices = unique(i -> content[i]["id_hash"], eachindex(content)) # returns the index the first occurrence of an id_hash
    unique_content = content[unique_indices]

    # length diff 
    start_l = length(content)
    end_l = length(unique_content)
    diff = start_l - end_l

    # Log the duplicates 
    if diff != 0
        @info "Number of non unique nodes ids in content : $(diff)"
    end

    return unique_content
end

"""
    write_only_new_to_nodes(con::DuckDB.DB, content::OrderedDict{String,V}) where {V}
"""
function write_only_new_to_nodes!(
    con::DuckDB.DB,
    content::Vector{OrderedDict{String,V}},
) where {V}
    # New rows proposals
    unique_content = _get_unique_from_content(content)
    ids_future = _get_ids_from_content(unique_content)
    # get the ones already in table 
    ids_current = DataFrame(_execute_command(con, "SELECT id_hash from NODES"))
    # filter to get only the new ones
    new_ids = setdiff(Set(ids_future), ids_current[!, "id_hash"]) # from the potential new entries, remove those that already exist
    new_content = [i for i in unique_content if i["id_hash"] in new_ids] # its ok since we only have unique potential new entries
    # write if any
    result = write_to_nodes!(con, new_content)
    return result
end

"""
    write_to_edges(con::DuckDB.DB, content::Vector{Dict{String,Any}})

Uses the Appender API to insert rows into the EDGES table.
 
Every value is inserted one by one in the Appender, as such, all data is expected to be present in each element of `content`.

See : `_append_content_to_appender`
"""
function write_to_edges!(con::DuckDB.DB, content::Vector{OrderedDict{String,V}}) where {V}
    # append data by row
    appender = DuckDB.Appender(con, "EDGES")
    _append_content_to_appender(appender, content)
    result = _manual_write_close_appender(appender)
    # result = DBInterface.close!(appender)
    return result
end

"""
    write_to_edges!(con::DuckDB.DB, content::Dict{String,Any})

Calls `write_to_edges!` with a vector of length 1 composed only of the dict `content`.
"""
function write_to_edges!(con::DuckDB.DB, content::OrderedDict{String,V}) where {V}
    write_to_edges!(con, [content])
end
