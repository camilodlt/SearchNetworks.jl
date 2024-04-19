# DROP 
# DUPLICATES 
# SearchNetworks._execute_command(con,"Select * from (Select * from db.nodes UNION all  Select * from db2) QUALIFY row_number() over (partition by id_hash order by id_hash) = 1") |> DataFrame

function _make_names_for_imported_dbs(n_imported_dbs::Int)
    return ["db" * string(i) for i = 1:n_imported_dbs]
end

function load_multiple_dbs!(con::DuckDB.DB, dbs_filenames::Vector{String})
    n_dbs = length(dbs_filenames)
    names_for_dbs = _make_names_for_imported_dbs(n_dbs)
    for (filename, db_name) in zip(dbs_filenames, names_for_dbs)
        _execute_command(con, "ATTACH '$filename' AS $db_name")
    end
end


function _nodes_union_query(names_of_dbs::Vector{String})
    select_from_dbs = ["Select * from $name.NODES" for name in names_of_dbs]
    inner_part = join(select_from_dbs, " UNION ALL ")
    query = "Select * from ($inner_part)"
    return query
end
function _edges_union_query(names_of_dbs::Vector{String})
    select_from_dbs = ["Select * from $name.EDGES" for name in names_of_dbs]
    inner_part = join(select_from_dbs, " UNION ALL ")
    query = "Select * from ($inner_part)"
    return query
end

function _dbs_names(con::DuckDB.DB)
    tables = SearchNetworks._execute_command(con, "SHOW DATABASES") |> DataFrame
    dbs = filter(e -> e != "memory" && occursin("db", e), tables[!, "database_name"])
    @assert length(dbs) > 0 "There are no databases aside from memory. Did you load several ?"
    return identity.(dbs)
end

function _switch_to_memory(con::DuckDB.DB)
    SearchNetworks._execute_command(con, "use memory")
    @debug "Switched to memory"
end

function log_nrows_in_table(con::DuckDB.DB, table_name::String)
    nrows_tmp =
        SearchNetworks._execute_command(con, "SELECT COUNT(*) FROM $table_name") |>
        DataFrame
    nrows = nrows_tmp[1, 1]
    @debug "Table $table_name has $nrows rows"
end
function _drop_tmp_table!(con::DuckDB.DB)
    SearchNetworks._execute_command(con, "DROP TABLE tmp")
    @debug "Table tmp was just dropped"
end

function _union_nodes_from_dbs_to_tmp!(con::DuckDB.DB, dbs_names::Vector{String})
    @debug "Union of all dbs NODES : $dbs_names"
    query = "CREATE TABLE tmp AS ($(_nodes_union_query(dbs_names)))"
    r = SearchNetworks._execute_command(con, query)
    log_nrows_in_table(con, "TMP")
end

function _union_edges_from_dbs_to_edges!(con::DuckDB.DB, dbs_names::Vector{String})
    @debug "Union of all dbs EDGES : $dbs_names"
    query = "CREATE TABLE edges AS ($(_edges_union_query(dbs_names)))"
    SearchNetworks._execute_command(con, query)
    log_nrows_in_table(con, "EDGES")
end

function _nodes_after_remove_duplicates_from_tmp!(con::DuckDB.DB)
    SearchNetworks._execute_command(
        con,
        "CREATE TABLE NODES AS Select * from tmp QUALIFY row_number() over (partition by id_hash) = 1 ORDER BY id_hash",
    ) # for each group, picks the first line 
    # group are individuals with the same id_hash.
    log_nrows_in_table(con, "NODES")
end

function _assert_individuals_with_eq_hashes_are_eq!(con::DuckDB.DB)
    # assert there was no false duplicates
    #    a false duplicate is one that has the same "id_hash" but different information
    #    but that is nonsensical, bc since the id_hash is the unique identifier, the same id_hash can not yield two different rows
    all_column_names =
        SearchNetworks._execute_command(con, "Select * from tmp limit 1") |>
        DataFrame |>
        names
    names_for_anti_join = join(all_column_names, ", ")
    # this gives individuals who are different than those kept in NODES
    # If the individual is different, that means that in the partition when one 1 was picked to form the NODES table, another row in the partition was actually different
    wrong_individuals =
        SearchNetworks._execute_command(
            con,
            "SELECT * FROM tmp ANTI JOIN nodes USING ($names_for_anti_join )",
        ) |> DataFrame
    if nrow(wrong_individuals) > 0
        @debug wrong_individuals
        throw(
            AssertionError("The dataset had wrong duplicates. See the debug information."),
        )
    end
end

function combine_independent_dbs!(con::DuckDB.DB)
    dbs = _dbs_names(con) # db1, db2 ...
    # Switch to memory
    _switch_to_memory(con)

    # UNION ALL THE NODES --- ---

    _union_nodes_from_dbs_to_tmp!(con, dbs) # tmp table is the union of all nodes
    # Non duplicates union
    _nodes_after_remove_duplicates_from_tmp!(con)
    # assert there was no false duplicates
    _assert_individuals_with_eq_hashes_are_eq!(con)
    # tmp (the union all) is no longer needed since we have the filtered down table
    _drop_tmp_table!(con)

    # UNION ALL THE EDGES --- ---
    _union_edges_from_dbs_to_edges!(con, dbs) # duplicates are normal in edges.

end
