import argparse
import logging
import re
import sys
from collections.abc import *
from typing import Union

import polars as pl
from tqdm import tqdm

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
        description=r"""Condense edge semantics. 
        Use this script to import relevant documents for processing edge semantics in SemMedDB""",
        usage="03_Condense_edge_semmantics_polars.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-v",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )

    parser.add_argument(
        "-n",
        "--drop_negative_relations",
        default=False,
        action="store_true",
        help='Remove negative relations from output. If "convert_negative_relations" is True, negative relations will be converted to bidirectional',
    )
    parser.add_argument(
        "-c",
        "--convert_negative_relations",
        default=False,
        action="store_true",
        help='Converts negative relations to bidirectional relations. i.e. "NEG_ISA" -> "ISA"',
    )

    return parser.parse_args(args)


#### Condense Edge Semmantics
def main(args):
    logger.info("Running 03_Condense_edge_semantics.py")
    logger.info("... Importing Nodes and Edges file")
    nodes = pl.read_parquet(f"../data/nodes_{args.semmed_version}_consolidated.parquet")
    edges = pl.read_parquet(f"../data/edges_{args.semmed_version}_consolidated.parquet")

    # remove abbrev and revabbrev, because they're incorrect now.
    edges = edges[
        [
            "h_id",
            "t_id",
            "edge",
            "start_type",
            "end_type",
            "type_type",
            "type_dir",
            "pmid",
            "n_pmids",
        ]
    ]
    logger.info("... clean up edge types and abbreviations")
    edges = edges.with_columns(
        (
            pl.col("edge")
            + "_"
            + pl.col("start_type")
            + pl.col("type_type")
            + pl.col("type_dir")
            + pl.col("end_type")
        ).alias("r"),
        (
            pl.col("start_type")
            + pl.col("type_type")
            + pl.col("type_dir")
            + pl.col("end_type")
        ).alias("abbrev"),
        (
            pl.col("end_type")
            + pl.col("type_dir")
            + pl.col("type_type")
            + pl.col("start_type")
        ).alias("rev_abbrev"),
    )

    if args.drop_negative_relations or args.convert_negative_relations:
        logger.info("... Condensing edge semantics")

        edge_map = pl.read_csv("../data/edge_condense_map.csv")

        # create mapping dict from groupping the fromtypes to the totypes
        # map the dict to a new column
        # if the old edge requires swapping (swap=True), query for those items, swap, and restack

        # way faster, less type changing since conversion happens 1 time.
        edges_consolidated3 = change_edge_type2(
            edges,
            edge_map["original_edge"].to_list(),
            edge_map["condensed_to"].to_list(),
            edge_map["reverse"].to_list(),
        )

        logger.info(
            f"... Edge count prior to consolidating relation types: {edges.shape[0]:,}"
        )
        logger.info(
            f"... Edge count after consolidating relation types: {edges_consolidated3.shape[0]:,}"
        )
    else:
        logger.info(
            "... No edge semantics to condense as `--drop_negative_relations` and/or `--convert_negative_relations` are False."
        )
        edges_consolidated3 = (
            edges.rename(
                {
                    "start_type": "htype",
                    "end_type": "ttype",
                    "type_type": "r_type",
                    "type_dir": "r_dir",
                }
            )
            .drop_nulls("r")
            .drop("old_r")
        )

    #### Remove duplicated undirected edges
    logger.info("Removing duplicated undirected edges")
    # check all edge ids in nodes
    assert (
        tmp := edges_consolidated3.filter(~pl.col("h_id").is_in(nodes["id"])).shape[0]
    ) == 0, f"... There are {tmp:,} edges that have head node ids not in nodes"  # no unlabeled head nodes
    assert (
        tmp := edges_consolidated3.filter(~pl.col("t_id").is_in(nodes["id"])).shape[0]
    ) == 0, f"... There are {tmp:,} edges that have tail node ids not in nodes"  # no unlabeled tail nodes

    abbrev_dict = create_acronym_dict(edges_consolidated3)
    # remove direction from the dictionary
    abbrev_dict = dict(
        zip(
            abbrev_dict.keys(),
            [i.replace(">", "") if ">" in i else i for i in abbrev_dict.values()],
        )
    )

    logger.info(f"... replacing duplicated undirected edges after edge consolidation")
    edges = edges_consolidated3.with_columns(
        pl.col("r").map_elements(lambda e: "_".join(e.split("_")[:-1])).alias("sem"),
        pl.col("r").map_elements(lambda e: e.split("_")[-1]).alias("abbrev"),
    ).with_columns(pl.col("sem").replace(abbrev_dict).alias("rtype"))

    edges = edges.with_columns(
        pl.when(pl.col("abbrev").str.contains(">"))
        .then(pl.col("htype") + pl.col("rtype") + ">" + pl.col("ttype"))
        .otherwise(pl.col("htype") + pl.col("rtype") + pl.col("ttype"))
        .alias("calc_abbrev")
    )
    # ensures that the abbreviations are correct because we removed the direction, and reapplied a relation conversion
    # edges = edges.drop("abbrev").rename({"calc_abbrev": "abbrev"})
    logger.info(f"... checking transformation with some assertions")

    # import pdb

    # pdb.set_trace()
    # # check your transformation
    assert (
        sum(edges["calc_abbrev"] != edges["abbrev"]) == 0
    ), "Some type mappings are incorrect."

    # check typing for all nodes, and make sure there are no nodes with multiple types
    assert (
        pl.concat(
            [
                edges[["h_id", "htype"]].rename({"h_id": "id", "htype": "type"}),
                edges[["t_id", "ttype"]].rename({"t_id": "id", "ttype": "type"}),
            ]
        )
        .unique()
        .group_by("id")
        .agg("type")
        .with_columns(
            pl.col("type").list.unique(),
            pl.col("type").list.unique().list.len().alias("count"),
        )
        .filter(pl.col("count") > 1)
    ).shape[0] == 0, "There are nodes with multiple types"

    #### Undirected edges between two nodes of the same type should only have 1 instance
    logger.info("Remove duplicate edges between nodes of the same type...")
    # Get the edges that are un-directed, between same type
    self_ref_df = edges.filter(
        (pl.col("htype") == pl.col("ttype")) & (~pl.col("abbrev").str.contains(">"))
    )

    og_self_ref_df = self_ref_df.shape
    # create the complement of the self_ref_df
    non_self_ref_df = edges.join(
        self_ref_df, on=["h_id", "t_id", "sem", "rtype", "abbrev"], how="anti"
    ).drop("")
    logger.info(f"... dropping duplicates of non-directional edge types")

    # For self referential edge types without direction, sort the h_id and t_id and drop duplicates
    # group by h_id,t_id,old_r,abbrev,rev_abbrev,is_neg,sem,rtype,calc_abbrev
    # aggregate the pmid and recount the number of pmids
    # 1 min 45s -> 2.3s!
    self_ref_df = (
        self_ref_df.explode("pmid")
        .unique(["pmid", "rtype", "h_id", "t_id"])
        .with_columns(
            pl.when(pl.col("h_id") > pl.col("t_id"))
            .then(pl.col("t_id"))
            .otherwise(pl.col("h_id"))
            .alias("h_id"),
            pl.when(pl.col("t_id") > pl.col("h_id"))
            .then(pl.col("t_id"))
            .otherwise(pl.col("h_id"))
            .alias("t_id"),
        )
        .group_by(
            [
                "h_id",
                "t_id",
                # "old_r",
                "r",
                "htype",
                "ttype",
                "r_type",
                "r_dir",
                "abbrev",
                "rev_abbrev",
                "sem",
                "rtype",
            ]
        )
        .agg("pmid")
        .with_columns(pl.col("pmid").list.len().alias("n_pmids"))
    )[
        [
            "h_id",
            "t_id",
            # "old_r",
            "pmid",
            "n_pmids",
            "r",
            "htype",
            "ttype",
            "r_type",
            "r_dir",
            "abbrev",
            "rev_abbrev",
            "sem",
            "rtype",
        ]
    ]
    logger.info(
        f"... Before De-duplication: {og_self_ref_df[0]:,} Edges between nodes of the same type"
    )
    logger.info(
        f"... After De-duplication: {self_ref_df.shape[0]:,} Edges between nodes of the same type"
    )

    # drop duplicates from non_self_ref_df
    # merge the self_ref_df and non_self_ref_df

    new_edges = pl.concat([self_ref_df, non_self_ref_df], how="diagonal").unique(
        ["h_id", "t_id", "sem", "rtype", "abbrev"]
    )

    logger.info(f"... {len(edges):,} edges before deduplication")
    logger.info(f"... {len(new_edges):,} edges after deduplication")

    logger.info(f"... double checks to ensure no nodes with multiple types")
    # check typing for all nodes, and make sure there are no nodes with multiple types
    assert (
        pl.concat(
            [
                new_edges[["h_id", "htype"]].rename({"h_id": "id", "htype": "type"}),
                new_edges[["t_id", "ttype"]].rename({"t_id": "id", "ttype": "type"}),
            ]
        )
        .unique()
        .group_by("id")
        .agg("type")
        .with_columns(
            pl.col("type").list.unique(),
            pl.col("type").list.unique().list.len().alias("count"),
        )
        .filter(pl.col("count") > 1)
    ).shape[0] == 0, "There are nodes with multiple types"

    logger.info("... relabeling column names")
    # relabel the edge columns in the new_edges
    new_edges = new_edges.rename(
        {
            "pmid": "pmids",
            "r_dir": "rdir",
            "sem": "sem",
        }
    )[
        [
            "h_id",
            "t_id",
            "pmids",
            "n_pmids",
            "r",
            "htype",
            "ttype",
            "rtype",
            "rdir",
            "abbrev",
            "rev_abbrev",
            "sem",
        ]
    ]
    #### Finish de-duplication and merge any pmids between those duplicated edges
    # check if any pmids are duplicated by expanding the pmids and grouping by the columns
    assert (
        new_edges.shape[0]
        == new_edges.explode("pmids")
        .group_by(
            [
                "h_id",
                "t_id",
                "r",
                "htype",
                "ttype",
                "rtype",
                "rdir",
                "abbrev",
                "rev_abbrev",
                "sem",
            ]
        )
        .agg("pmids")
        .with_columns(pl.col("pmids").list.len().alias("n_pmids"))
        .shape[0]
    ), "Some pmids are duplicated and you should deduplicate by performing a groupby"

    #### Export files

    logger.info(
        f"... exporting processed edges at: '../data/edges_{args.semmed_version}_consolidated_condensed.parquet'"
    )
    # save the new_edges
    new_edges.sort("r").write_parquet(
        f"../data/edges_{args.semmed_version}_consolidated_condensed.parquet"
    )
    # save the nodes (even though we didn't do anything to it)
    nodes.sort("label").write_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated_condensed.parquet"
    )

    logger.info("Complete processing 03_Condensing_edge_semantics.py\n")


#### Functions for condensing edge semantics
def change_edge_type2(
    edges: pl.DataFrame,
    from_type: Iterable[str],
    to_type: Iterable[str],
    reverse: Iterable[str],
) -> pl.DataFrame:
    type_map = dict(zip(from_type, to_type))  # from_type to to_type map
    rev_map = dict(
        zip(from_type, [str(i) for i in reverse])
    )  # from_type to reverse map
    # map type and reverse to columns new_type and swap
    edges = edges.with_columns(
        pl.col("r").replace(type_map).alias("new_r"),
        pl.col("r").replace(rev_map).alias("swap"),
    )
    # filter swaps and replace the swaps. restack
    edges_swap = edges.filter(pl.col("swap") == "True").with_columns(
        pl.col("h_id").alias("t_id"),
        pl.col("t_id").alias("h_id"),
        pl.col("start_type").alias("end_type"),
        pl.col("end_type").alias("start_type"),
    )[
        [
            "h_id",
            "t_id",
            "r",
            "pmid",
            "n_pmids",
            "new_r",
            "start_type",
            "end_type",
            "type_type",
            "type_dir",
            "abbrev",
            "rev_abbrev",
        ]
    ]  # cols must be in order to stack
    edges_no_swap = edges.filter(pl.col("swap") == "False")[
        [
            "h_id",
            "t_id",
            "r",
            "pmid",
            "n_pmids",
            "new_r",
            "start_type",
            "end_type",
            "type_type",
            "type_dir",
            "abbrev",
            "rev_abbrev",
        ]
    ]

    edges_swap = edges_swap.vstack(edges_no_swap)
    edges_swap = edges_swap.rename(
        {
            "r": "old_r",
            "new_r": "r",
            "start_type": "htype",
            "end_type": "ttype",
            "type_type": "r_type",
            "type_dir": "r_dir",
        }
    ).drop_nulls("r")

    return edges_swap


def create_acronym_dict(edges: pl.DataFrame) -> dict:
    """
    Given edges dataframe, extract relations, to create a dictionary of label abbreviations
    """
    # check for appropriate columns
    assert (
        "htype" in edges.columns or "ttype" not in edges.columns
    ), "Node types not in edges. Use get_node_types() to add them"

    # get unique edge relations and split them up
    edges = (
        edges.group_by("r")
        .agg(["htype", "ttype"])
        .with_columns(
            pl.col("htype").list.first(),
            pl.col("ttype").list.first(),
            pl.col("htype").list.unique().list.len().alias("hlen"),
            pl.col("ttype").list.unique().list.len().alias("tlen"),
        )
    )

    # check if each htype and ttype has only one unique value
    assert (
        tmp := edges.filter(pl.col("hlen") > 1).shape[0]
    ) == 0, f"... There are {tmp:,} edges with multiple htypes"
    assert (
        tmp := edges.filter(pl.col("tlen") > 1).shape[0]
    ) == 0, f"... There are {tmp:,} edges with multiple ttypes"

    edges = edges.drop(["hlen", "tlen"])

    # Extracts relation abbreviations from relations
    edges = edges.with_columns(
        x := pl.col("r")
        .str.extract_all("[A-Za-z><]+")
        .alias(
            "split_rel"
        ),  # splits relations into components. ex: "PART_OF_PHpoCD" -> ["PART", "OF", "PHpoCD"]
        x.list.get(-1).alias(
            "mtypes"
        ),  # gets the last component of the prev. list ex: "PHpoCD"
    ).with_columns(
        (x := pl.col("mtypes").str.extract_all("[A-Z]+"))
        .list.get(0)
        .alias("h_abbv"),  # gets the h_abbv. ex: "PH"
        x.list.get(1).alias("t_abbv"),  # gets the t_abbv. ex: "CD"
        pl.col("mtypes")
        .str.extract_all("[a-z><]+")
        .list.get(0)
        .alias("rel_abbv"),  # gets the rel_abbv. ex: "po"
        pl.when(pl.col("split_rel").list.len() > 2)  # gets the rel. ex: "PART_OF"
        .then(
            pl.col("split_rel")
            .list.slice(
                0, pl.col("split_rel").list.len() - 1
            )  # if there are more than 2 components, join them
            .list.join("_")
        )
        .otherwise(pl.col("split_rel").list.first())
        .alias("rel"),
    )
    # get unique relations and their abbreviations
    edges = edges.unique(["rel", "rel_abbv"])
    # assign a dictionary of relations and their abbreviations
    rel_dict = dict(zip(edges["rel"], edges["rel_abbv"]))

    return rel_dict


if __name__ == "__main__":
    main(parse_args())
