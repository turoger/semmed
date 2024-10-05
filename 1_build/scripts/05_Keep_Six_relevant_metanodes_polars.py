import argparse
import logging
import sys

import polars as pl

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s] \t %(message)s", "%Y-%m-%d %H:%M:%S")

# create console handler and set level to info
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


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
    logger.info(f"Running 05_Keep_Six_relevant_metanodes.py")
    logger.info(f"... Importing nodes and edges")
    edges = pl.read_parquet(
        f"../data/edges_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )
    nodes = pl.read_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )
    logger.info(f"... checking all nodes are of only one type")
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

    logger.info(f"... checking all node types are the same between edges")
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
    logger.info("... removing less-useful metanodes")
    # Remove nodes of types that are less-useful
    logger.info(f"... Nodes prior to removal of less-useful types: {nodes.shape[0]:,}")
    remove_types = [
        "Organizations",
        "Activities & Behaviors",
        "Concepts & Ideas",
        "Procedures",
        "Devices",
        "Living Beings",
    ]
    nodes = nodes.filter(~pl.col("label").is_in(remove_types))
    logger.info(f"... Nodes after removal of less-useful types: {nodes.shape[0]:,}")

    # Filter edges after removing less-useful types

    logger.info(
        f"... Edges prior to removal of abstract Node types: {edges.shape[0]:,}"
    )
    edges = edges.filter(
        pl.col("h_id").is_in(nodes["id"]), pl.col("t_id").is_in(nodes["id"])
    )
    logger.info(f"... Edges after removal of abstract Node types: {edges.shape[0]:,}\n")

    logger.info(f"Number of unique Node Types: {nodes['label'].n_unique()}")
    logger.info(
        f"Node Type                      Count:\n---------------------------------------\
    \n{nodes['label'].value_counts().sort('count', descending=True)}"
    )

    logger.info(f"Number of unique Edges Types: {edges['sem'].n_unique()}")
    logger.info(
        f"Edge Type                 Count:\n-----------------------------------\
    \n{edges['sem'].value_counts().sort('count', descending=True)}"
    )

    logger.info(
        f"... Writing output file to ../data/nodes_{args.semmed_version}_cons_6_metanode.parquet and ../data/edges_{args.semmed_version}_cons_6_metanode.parquet\n"
    )
    nodes.write_parquet(f"../data/nodes_{args.semmed_version}_cons_6_metanodes.parquet")
    edges.write_parquet(f"../data/edges_{args.semmed_version}_cons_6_metanodes.parquet")

    logger.info(f"Complete processing 05_Keep_Six_relevant_metanodes.py\n")


if __name__ == "__main__":
    main(parse_args())
