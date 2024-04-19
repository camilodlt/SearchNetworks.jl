"""

Info about the toy dbs in assets : 

test_duplicates which has 2 nodes with id hash : 
    - id1 
    - id2

Connexions: 
    - id1 => id1
    - id1 => id2 

test_duplicates_1 which has 2 nodes with id hash : 
    - id1 
    - id2

Connexions: 
    - id1 => id1
    - id1 => id2 


test_duplicates_2 which has 1 node with id hash : 
    - id1 

Connexions : 
    - id1 => id1

A problematic DB : 
test_duplicates_3 which has 1 node with id hash : 
    - id1 BUT the hash in column 'a' is different from that in test_duplicates db

A connection between id1 and id1

With this DB, the combine method should fail, because two rows with the same ids did not have the same hashes for other columns
And that is incoherent, because the id_hash represents the unique identifier for every individual
Two individuals with the same id_hash should be the same in all regards. 

"""

@testset "Post Processing" begin
    @test begin # UNION of DBS
        con = create_DB()
        load_multiple_dbs!(
            con,
            ["assets/test_duplicates.db", "assets/test_duplicates_2.db"],
        )
        # check that the dbs were loaded 
        nodes1 = DataFrame(SearchNetworks._execute_command(con, "Select * FROM db1.NODES"))
        nodes2 = DataFrame(SearchNetworks._execute_command(con, "Select * FROM db2.NODES"))
        c1 = nrow(nodes1) == 2
        c2 = nrow(nodes2) == 1
        close(con)
        c1 && c2

    end

    @test begin # Union 2 databases. Nodes should not be duplicated
        con = create_DB()
        load_multiple_dbs!(
            con,
            ["assets/test_duplicates.db", "assets/test_duplicates_2.db"],
        )
        combine_independent_dbs!(con) # switches to main db
        nodes = get_nodes_from_db(con)
        edges = get_edges_from_db(con)
        # Verification on Nodes
        c1 = nrow(nodes) == 2 # The nodes db should only have 2 nodes : id1, id2 and not two times the id1
        c2 = nodes[1, "id_hash"] == "id1"
        c3 = nodes[2, "id_hash"] == "id2"
        # Verification on EDGES
        c4 = nrow(edges) == 3
        c5 = edges[1, "_from"] == "id1" && edges[1, "_to"] == "id1" # coming from db1
        c6 = edges[2, "_from"] == "id1" && edges[2, "_to"] == "id2" # coming from db1
        c7 = edges[3, "_from"] == "id1" && edges[3, "_to"] == "id1" # coming from db2
        c1 && c2 && c3 && c4 && c5 && c6 && c7
    end

    @test begin # Union 3 databases. Nodes should not be duplicated
        """
        The only nodes should be : 
            - id1 # coming from DB1, DB2, DB3
            - id2 # coming from DB1, DB2
            - id3 # coming only from DB2

        there should be 5 connexions: 
            - id1 => id1 # db1
            - id1 => id2 # db1

            - id1 => id1 # db2 
            - id1 => id2 # db2

            - id1 => id1 # db3
        """
        con = create_DB()
        load_multiple_dbs!(
            con,
            [
                "assets/test_duplicates.db",
                "assets/test_duplicates_1.db", # a copy of the above + node id3
                "assets/test_duplicates_2.db",
            ],
        )
        combine_independent_dbs!(con) # switches to memory db
        nodes = get_nodes_from_db(con)
        edges = get_edges_from_db(con)
        # Verification on Nodes
        c1 = nrow(nodes) == 3 # The nodes db should only have 2 nodes : id1, id2 and not two times the id1
        c2 = nodes[1, "id_hash"] == "id1"
        c3 = nodes[2, "id_hash"] == "id2"
        c3_bis = nodes[3, "id_hash"] == "id3" && nodes[3, "a"] == "b"

        # Verification on EDGES
        c4 = nrow(edges) == 5
        # DB1
        c5 = edges[1, "_from"] == "id1" && edges[1, "_to"] == "id1" # coming from db1
        c6 = edges[2, "_from"] == "id1" && edges[2, "_to"] == "id2" # coming from db1
        # DB1
        c7 = edges[3, "_from"] == "id1" && edges[3, "_to"] == "id1" # coming from db1
        c8 = edges[4, "_from"] == "id1" && edges[4, "_to"] == "id2" # coming from db1
        # DB1
        c9 = edges[5, "_from"] == "id1" && edges[5, "_to"] == "id1" # coming from db1
        c1 && c2 && c3 && c3_bis && c4 && c5 && c6 && c7 && c8 && c9
    end

    @test_throws AssertionError begin # THIS HAS TO FAIL.
        """ 
        DB1 has a row with id : id1 and some properties 
        DB4 has a row with id : id1 and different properties

        Two rows with the same id can not be different. So this should fail
        """
        con = create_DB()
        load_multiple_dbs!(
            con,
            [
                "assets/test_duplicates.db",
                "assets/test_duplicates_1.db",
                "assets/test_duplicates_2.db",
                "assets/test_duplicates_3.db", # this is the problematic one ! 
            ],
        )
        combine_independent_dbs!(con) # switches to memory db
        nodes = get_nodes_from_db(con)
        edges = get_edges_from_db(con)
    end
end

# SearchNetworks._execute_command(con,"
#        with tmp_nodes_1 as (
#             select id_hash as id_hash_1,
#             row_number() over() as from_index from nodes
#         ),
#        tmp_nodes_2 AS (
#            select id_hash as id_hash_2,
#            row_number() over() as to_index from nodes
#            )

#        Select * FROM edges
#            JOIN tmp_nodes_1 ON
#                edges._from = tmp_nodes_1.id_hash_1
#            JOIN tmp_nodes_2 ON
#                edges._to = tmp_nodes_2.id_hash_2
#        ") |> DataFrame
