using DataStructures

# -- Test Summary

# -- -- CREATE DB IN MEMORY
@testset "DuckDB memory" begin
    # INTERNAL exec #
    @test begin
        con = DBInterface.connect(DuckDB.DB, ":memory:")
        qr = SearchNetworks._execute_command(con, "CREATE TABLE integers (i INTEGER)")
        typeof(qr) == DuckDB.QueryResult # it sent a correct QR
    end
end

# -- -- SN COL TYPE Instantiate the struct
@testset "SN col type" begin
    # API : col types # 
    @test begin # only string is true
        col = SearchNetworks.SN_col_type(string = true)
        col.string && !col.float
    end
    @test begin # float is true
        col = SearchNetworks.SN_col_type(float = true)
        col.float && !col.string
    end
    @test_throws AssertionError begin # Because all false
        col = SearchNetworks.SN_col_type()
    end
    @test_throws AssertionError begin # Because more than one true
        col = SearchNetworks.SN_col_type(string = true, float = true)
    end
end


# -- -- CREATE THE QUERY THAT CREATES A TABLE
@testset "TABLE QUERY CREATION" begin
    # Internal extract cols info # 
    @test begin # extract col names and types (k,v)
        cols_ok = OrderedDict("a" => SearchNetworks.SN_col_type(string = true))
        c, t = SearchNetworks._extract_extra_cols_info(cols_ok)
        c == ["a"] && t == [SearchNetworks.SN_col_type(string = true)]
    end
    @test_throws AssertionError begin # some keywords are not accepted # TODO whole list 
        cols_not_ok = OrderedDict("_from" => SearchNetworks.SN_col_type(string = true))
        c, t = SearchNetworks._extract_extra_cols_info(cols_not_ok)
    end

    # Internal SN types => string or double 
    @test begin
        t = [
            SearchNetworks.SN_col_type(string = true),
            SearchNetworks.SN_col_type(float = true),
        ]
        types_str = SearchNetworks._convert_types_to_DB_format(t)
        types_str == ["VARCHAR", "DOUBLE"]

    end

    # Internal SN types => string # 
    @test begin # correct string is created # 2 args
        extra_cols = OrderedDict(
            "a" => SearchNetworks.SN_col_type(string = true),
            "b" => SearchNetworks.SN_col_type(float = true),
        )
        c, t = SearchNetworks._extract_extra_cols_info(extra_cols)
        types_str = SearchNetworks._convert_types_to_DB_format(t)
        str = SearchNetworks._create_extra_cols_suffix(c, types_str)
        str == "a VARCHAR, b DOUBLE"
    end
    @test begin # correct string is created # 1 arg
        extra_cols = OrderedDict("a" => SearchNetworks.SN_col_type(string = true))
        c, t = SearchNetworks._extract_extra_cols_info(extra_cols)
        types_str = SearchNetworks._convert_types_to_DB_format(t)
        str = SearchNetworks._create_extra_cols_suffix(c, types_str)
        str == "a VARCHAR"
    end

    # WRAPPER for Creating Extra query for extra cols
    @test begin
        extra_cols = OrderedDict("a" => SearchNetworks.SN_col_type(string = true))
        str = SearchNetworks._create_command_extra_cols(extra_cols)
        str == "a VARCHAR"
    end

    # CREATE NODES TABLE COMMAND
    @test begin # with 0 extra, 1 extra and 2 extra cols
        # No extra cols
        str = SearchNetworks._create_nodes_command(nothing)
        cond1 = str == "CREATE TABLE NODES (id_hash VARCHAR PRIMARY KEY, )"
        # 1 extra col
        extra_cols = OrderedDict("a" => SearchNetworks.SN_col_type(string = true))
        str = SearchNetworks._create_nodes_command(extra_cols)
        cond2 = str == "CREATE TABLE NODES (id_hash VARCHAR PRIMARY KEY, a VARCHAR)"
        # 2 extra cols
        extra_cols = OrderedDict(
            "a" => SearchNetworks.SN_col_type(string = true),
            "b" => SearchNetworks.SN_col_type(float = true),
        )
        str = SearchNetworks._create_nodes_command(extra_cols)
        cond3 =
            str == "CREATE TABLE NODES (id_hash VARCHAR PRIMARY KEY, a VARCHAR, b DOUBLE)"
        cond1 && cond2 && cond3
    end

    # Reserved Cols  # Should test all reserved cols # TODO V2
    @test_throws AssertionError begin
        extra_cols = OrderedDict("_from" => SearchNetworks.SN_col_type(string = true))
        str = SearchNetworks._create_nodes_command(extra_cols)
    end
    @test_throws AssertionError begin
        extra_cols = OrderedDict("_from" => SearchNetworks.SN_col_type(string = true))
        str = SearchNetworks._create_edges_command(extra_cols)
    end

    # CREATE EDGES TABLE COMMAND
    @test begin # with 0 extra, 1 extra and 2 extra cols
        # No extra cols
        str = SearchNetworks._create_edges_command(nothing)
        cond1 =
            str ==
            "CREATE TABLE EDGES (_from VARCHAR REFERENCES NODES(id_hash), _to VARCHAR REFERENCES NODES(id_hash), iteration INTEGER, )"
        # 1 extra col
        extra_cols = OrderedDict("a" => SearchNetworks.SN_col_type(string = true))
        str = SearchNetworks._create_edges_command(extra_cols)
        cond2 =
            str ==
            "CREATE TABLE EDGES (_from VARCHAR REFERENCES NODES(id_hash), _to VARCHAR REFERENCES NODES(id_hash), iteration INTEGER, a VARCHAR)"

        # 2 extra cols
        extra_cols = OrderedDict(
            "a" => SearchNetworks.SN_col_type(string = true),
            "b" => SearchNetworks.SN_col_type(float = true),
        )
        str = SearchNetworks._create_edges_command(extra_cols)
        cond3 =
            str ==
            "CREATE TABLE EDGES (_from VARCHAR REFERENCES NODES(id_hash), _to VARCHAR REFERENCES NODES(id_hash), iteration INTEGER, a VARCHAR, b DOUBLE)"
        cond1 && cond2 && cond3
    end
end

@testset "DuckDB persistent" begin
    # CREATE DB --- 
    @test begin # persistent
        con = create_DB("assets/test_db.duckdb")
        close(con)
        rm("assets/test_db.duckdb") # it'll error if the file does not exist
        true
    end
end

@testset "Create Tables without extra params" begin
    @test begin # in memory
        con = create_DB()
        close(con)
        typeof(con) == DuckDB.DB
    end

    # CREATES THE TABLES
    @test begin
        con = create_DB()
        create_SN_tables!(con) # memory
        close(con)
        true
    end
    # Check that the tables where created
    @test begin
        con = create_DB()
        create_SN_tables!(con) # memory
        edges, nodes = get_edges_from_db(con), get_nodes_from_db(con)
        edges_cols_ok = names(edges) == ["_from", "_to", "iteration"]
        nodes_cols_ok = names(nodes) == ["id_hash"]
        close(con)
        edges_cols_ok & nodes_cols_ok
    end
end

@testset "Create Tables with extra params" begin
    # Check that the tables where created
    @test begin # Table with extra 
        con = create_DB()
        create_SN_tables!(
            con,
            extra_edges_cols = OrderedDict("a" => SN_col_type(string = true)),
            extra_nodes_cols = OrderedDict("b" => SN_col_type(float = true)),
        ) # memory
        edges, nodes = get_edges_from_db(con), get_nodes_from_db(con)
        edges_cols_ok = names(edges) == ["_from", "_to", "iteration", "a"]
        nodes_cols_ok = names(nodes) == ["id_hash", "b"]
        close(con)
        edges_cols_ok & nodes_cols_ok
    end
end

@testset "Reserved keywords when creating tables" begin
    # Test the types of custom cols and mandatory cols??? V2# TODO
    @test begin # Table with extra but those keywords are reserved
        for reserved_name in ["id_hash", "edge_id", "_from", "_to", "iteration"]
            @test_throws AssertionError begin
                con = create_DB()
                create_SN_tables!(
                    con,
                    extra_nodes_cols = OrderedDict(
                        reserved_name => SN_col_type(string = true),
                    ),
                )
            end
        end
        true
    end
end

# # WRITE TO TABLE #

@testset "Write to memory table" begin
    # Edge is not accepted before Node is created
    @test begin # 
        con = create_DB()
        create_SN_tables!(con)
        unable_to_write =
            write_to_edges!(
                con,
                OrderedDict(
                    "from" => "id1",
                    "to" => "id2",
                    #"fitness" => 0.1,
                    "iteration" => 1,
                ),
            ) == DuckDB.DuckDBError
        close(con)
        unable_to_write
    end
    # Adding to times the same node yeilds a DuckDBError
    @test begin # 
        con = create_DB()
        create_SN_tables!(con)
        unable_to_write =
            write_to_nodes!(
                con,
                [OrderedDict("id_hash" => "id1"), OrderedDict("id_hash" => "id1")],
            ) == DuckDB.DuckDBError

        close(con)
        unable_to_write
    end
    # Edge is accepted now that the nodes are created
    @test begin # 
        con = create_DB()
        create_SN_tables!(con)
        write_to_nodes!(
            con,
            [OrderedDict("id_hash" => "id1"), OrderedDict("id_hash" => "id2")],
        )
        able_to_write =
            write_to_edges!(
                con,
                OrderedDict(
                    "from" => "id1",
                    "to" => "id2",
                    #"fitness" => 0.1,
                    "iteration" => 1,
                ),
            ) == DuckDB.DuckDBSuccess
        close(con)
        able_to_write
    end

    # Check the types ? # TODO V2

end
@testset "Checking duplicates entries for Insertion" begin
    # INTERNAL API FOR DUPLICATES ENTRIES # 
    @test begin # Get ids from rows of content 

        ids = [
            OrderedDict("id_hash" => "1"),
            OrderedDict("id_hash" => "1"),
            OrderedDict("id_hash" => "2"),
        ]
        SearchNetworks._get_ids_from_content(ids) == ["1", "1", "2"]
    end
    @test begin # Get unique records from content 
        records = [ # we use "i" to identified them later and check that we picked the first ocurrences
            OrderedDict("id_hash" => "1", "i" => 1),
            OrderedDict("id_hash" => "2", "i" => 3),
            OrderedDict("id_hash" => "1", "i" => 2),
            OrderedDict("id_hash" => "2", "i" => 4),
        ]

        unique_records = SearchNetworks._get_unique_from_content(records)
        index_ok = [i["i"] for i in unique_records] == [1, 3] # which are the first ocurrences, 2 and 4 are duplicates
        ids_ok = [i["id_hash"] for i in unique_records] == ["1", "2"] # which are the first ocurrences 
        index_ok && ids_ok
    end
end

@testset "WRITE ONLY NEW NODES" begin
    # write only new to nodes should also work if the db is empty
    @test begin
        con = create_DB()
        create_SN_tables!(
            con,
            extra_nodes_cols = OrderedDict("a" => SN_col_type(string = true)),
        )
        # write some nodes to begin with
        r = write_only_new_to_nodes!(
            con,
            [
                OrderedDict("id_hash" => "id1", "a" => "a"),
                OrderedDict("id_hash" => "id1", "a" => "b"), # this one should be ommitted
                OrderedDict("id_hash" => "id2", "a" => "a"),
            ],
        )
        df_nodes = get_nodes_from_db(con)
        nodes_ok = df_nodes[!, "id_hash"] == ["id1", "id2"]
        second_col_ok = df_nodes[!, "a"] == ["a", "a"] # the second row was ommitted because the id1 was already there
        close(con)
        nodes_ok && second_col_ok
    end

    # Only insert new nodes when some are already present
    @test begin # should not accept because of incorrect type
        con = create_DB()
        create_SN_tables!(
            con,
            extra_nodes_cols = OrderedDict("a" => SN_col_type(string = true)),
        )
        # write some nodes to begin with
        write_to_nodes!(
            con,
            [
                OrderedDict("id_hash" => "id1", "a" => "a"),
                OrderedDict("id_hash" => "id2", "a" => "a"),
            ],
        )
        # write only extra nodes # id3
        write_only_new_to_nodes!(
            con,
            [
                OrderedDict("id_hash" => "id1", "a" => "a"), # already present so omitted
                OrderedDict("id_hash" => "id1", "a" => "b"), # second id1 should be omitted. even if they have "b" as value in col "a".
                OrderedDict("id_hash" => "id2", "a" => "a"),
                OrderedDict("id_hash" => "id3", "a" => "a"), # first id3
                OrderedDict("id_hash" => "id3", "a" => "b"), # this one should be ommited
            ],
        )
        df_nodes = get_nodes_from_db(con)
        nodes_ok = df_nodes[!, "id_hash"] == ["id1", "id2", "id3"] # only id3 was added
        second_col_ok = df_nodes[!, "a"] == ["a", "a", "a"] # last row, column "a" 
        close(con)
        nodes_ok && second_col_ok
    end
end

# NO NEED NOW TO SAVE TO PARQUET BC DB CAN BE PERSISTENT
