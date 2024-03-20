import argparse

import polars as pl


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Keep Six Main Metanode Types. 
        Use this script to remove less useful node types and remove those nodes from the edge file in SemMedDB""",
        usage="05_Keep_Six_relevant_metanodes_polars.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-n",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )

    return parser.parse_args(args)


def main(args):
    print(f"Running 05_Keep_Six_relevant_metanodes.py")
    print(f"... Importing nodes and edges")
    edges = pl.read_parquet(
        f"../data/edges_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )
    nodes = pl.read_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )
    print(f"... checking all nodes are of only one type")
    # assertion to check if all nodes have one type only!
    assert (
        pl.concat(
            [
                edges[["h_id", "htype"]].rename({"h_id": "id", "htype": "type"}),
                edges[["t_id", "ttype"]].rename({"t_id": "id", "ttype": "type"}),
            ]
        )
        .group_by("id")
        .agg("type")
        .with_columns(
            pl.col("type").list.unique(),
            pl.col("type").list.unique().list.len().alias("type_count"),
        )
        .filter(pl.col("type_count") > 1)
        .shape[0]
        == 0
    ), "Some nodes have multiple types"

    print(f"... checking all node types are the same between edges")
    # Check if node types are the same between edges and nodes
    assert (
        dict(
            zip(
                (
                    e := (
                        pl.concat(
                            [
                                edges[["h_id", "htype"]].rename(
                                    {"h_id": "id", "htype": "type"}
                                ),
                                edges[["t_id", "ttype"]].rename(
                                    {"t_id": "id", "ttype": "type"}
                                ),
                            ]
                        )
                        .group_by("id")
                        .agg("type")
                        .with_columns(pl.col("type").list.unique().list.first())
                        .unique()
                    )
                )["id"].to_list(),
                e["type"].to_list(),
            )
        ).items()
        <= dict(
            zip(
                (n := nodes[["id", "abv_label"]].unique())["id"].to_list(),
                n["abv_label"].to_list(),
            )
        ).items()
    ), "Check all edge node labels are in node labels"

    #### Removing un-needed metanodes
    print("... removing less-useful metanodes")
    # Remove nodes of types that are less-useful
    print(f"... Nodes prior to removal of less-useful types: {nodes.shape[0]:,}")
    remove_types = [
        "Organizations",
        "Activities & Behaviors",
        "Concepts & Ideas",
        "Procedures",
        "Devices",
        "Living Beings",
    ]
    nodes = nodes.filter(~pl.col("label").is_in(remove_types))
    print(f"... Nodes after removal of less-useful types: {nodes.shape[0]:,}")

    # Filter edges after removing less-useful types

    print(f"... Edges prior to removal of abstract Node types: {edges.shape[0]:,}")
    edges = edges.filter(
        pl.col("h_id").is_in(nodes["id"]), pl.col("t_id").is_in(nodes["id"])
    )
    print(f"... Edges after removal of abstract Node types: {edges.shape[0]:,}\n")

    print(f"Number of unique Node Types: {nodes['label'].n_unique()}")
    print(
        f"Node Type                      Count:\n---------------------------------------\
    \n{nodes['label'].value_counts().sort('counts', descending=True)}"
    )

    print(f"Number of unique Edges Types: {edges['sem'].n_unique()}")
    print(
        f"Edge Type                 Count:\n-----------------------------------\
    \n{edges['sem'].value_counts().sort('counts', descending=True)}"
    )

    print(
        f"... Writing output file to ../data/nodes_{args.semmed_version}_cons_6_metanode.parquet and ../data/edges_{args.semmed_version}_cons_6_metanode.parquet\n"
    )
    nodes.write_parquet(f"../data/nodes_{args.semmed_version}_cons_6_metanodes.parquet")
    edges.write_parquet(f"../data/edges_{args.semmed_version}_cons_6_metanodes.parquet")

    print(f"Complete processing 05_Keep_Six_relevant_metanodes.py\n")


if __name__ == "__main__":
    main(parse_args())
