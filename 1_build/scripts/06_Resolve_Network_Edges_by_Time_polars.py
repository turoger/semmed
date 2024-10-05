import datetime
import logging
import os
import pickle
import sys
import warnings

import polars as pl
from tqdm import tqdm

warnings.filterwarnings("ignore")
# from hetnet_ml import graph_tools as gt
import argparse

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
        description=r"""Split dataset by time. 
        Use this script to create nodes/edges files for each year in SemMedDB.""",
        usage="06_Resolve_Network_Edges_by_Time_polars.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-n",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )

    parser.add_argument(
        "-b",
        "--base_dir",
        default="../data/time_networks-6_metanode",
        type=str,
        help="directory to build the time-based dataset",
    )

    return parser.parse_args(args)


def get_year_category(diff):
    if diff > 20:
        return "20+ After"
    elif diff >= 15 and diff < 20:
        return "15-20 After"
    elif diff >= 10 and diff < 15:
        return "10-15 After"
    elif diff >= 5 and diff < 10:
        return "5-10 After"
    elif diff >= 0 and diff < 5:
        return "0-5 After"
    elif diff >= -5 and diff < 0:
        return "0-5 Before"
    elif diff >= -10 and diff < -5:
        return "5-10 Before"
    elif diff >= -15 and diff < -10:
        return "10-15 Before"
    elif diff >= -20 and diff < -15:
        return "15-20 Before"
    elif diff < -20:
        return "20+ Before"


def remove_colons(df: pl.DataFrame) -> pl.DataFrame:
    """
    given a polars dataframe, reassign column names without colons and turn to lower case
    """
    df_col_names = df.columns
    df.columns = [name.replace(":", "").lower() for name in df_col_names]
    return df


def main(args):
    logger.info("")
    logger.info("")
    logger.info("Running 06_Resolve_Network_Edges_by_Time")

    #### Build a final ID to year Map
    logger.info("... Load PMID to year from NLM, PMC, EUR, and EBI")
    nlm = pickle.load(open("../data/pmid_to_year_NLM.pkl", "rb"))
    pmc = pickle.load(open("../data/pmid_to_year_PMC.pkl", "rb"))
    eur = pickle.load(open("../data/pmid_to_year_Eur.pkl", "rb"))
    ebi = pickle.load(open("../data/pmid_to_year_EBI.pkl", "rb"))

    logger.info("... Reformatting all loaded PMID from str:str to str:ID")
    # NLM
    for k, v in tqdm(nlm.items()):
        nlm[k] = int(v)
    # EUR
    rem_index = list()
    for k, v in tqdm(eur.items()):
        try:
            eur[k] = int(v)
        except:
            rem_index.append(k)

    for k in rem_index:
        del eur[k]
    # EBI
    for k, v in tqdm(ebi.items()):
        ebi[k] = int(v.split("-")[0])
    # PMC
    # order of importance right to left. (pmc values will replace all others)
    id_to_year = {**eur, **nlm, **ebi, **pmc}
    id_to_year = {str(k): str(v) for k, v in id_to_year.items()}

    logger.info(f"... Getting nodes file")

    nodes = pl.read_parquet(
        source=f"../data/nodes_{args.semmed_version}_cons_6_metanodes.parquet"
    )
    logger.info(f"... Getting edges file")
    edges = pl.read_parquet(
        source="../data/edges_VER43_R_cons_6_metanodes.parquet",
    )

    logger.info(f"... Getting indications file")
    indications = pl.read_parquet(
        source="../data/indications_nodemerge.parquet"
    ).drop_nulls("approval_year")

    logger.info(f"... adding publication dates to edges")
    # add years to edges
    # turn pmids into a list so we can work with it
    # explode pmids
    edges = (
        edges.explode("pmids")  # uncollapse list in dataframe
        .with_columns(  # turn uncollapsed list from Int to Str, replace via dictionary, and reconvert back to Int
            pl.col("pmids")
            .cast(pl.Utf8)
            .replace(id_to_year)
            .cast(pl.Int64)
            .alias("pub_years")
        )
        .group_by(
            [
                "h_id",
                "t_id",
                "r",
                "n_pmids",
                "htype",
                "ttype",
                "rtype",
                "rdir",
                "abbrev",
                "sem",
            ]
        )  # regroup the dataframe
        .agg(["pmids", "pub_years"])
        .with_columns(
            pl.col("pub_years").list.min().alias("first_pub")
        )  # get min of pub_years
    )

    for year in (pbar := tqdm(range(1950, 2024, 1))):
        # Define the save directory
        out_dir = os.path.join(args.base_dir, str(year))

        # Make sure the save directory exists, if not, make it
        try:
            os.stat(out_dir)
        except:
            os.makedirs(out_dir)

        # Filter the edges by year
        e_filt = edges.filter(pl.col("first_pub") <= year)

        # Keep only nodes that have edges joining them
        node_ids = set(e_filt["h_id"]).union(set(e_filt["t_id"]))
        n_filt = nodes.filter(pl.col("id").is_in(list(node_ids)))

        # Keep only indications that have both the compound and disease still existing in the network
        ind_filt = (
            indications.filter(
                pl.col("compound_semmed_id").is_in(list(node_ids)),
                pl.col("disease_semmed_id").is_in(list(node_ids)),
            )  # Determine the difference between the current year and approval
            .with_columns((pl.col("approval_year").cast(int) - year).alias("year_diff"))
            .with_columns(
                (pl.col("year_diff")).map_elements(get_year_category).alias("year_cat")
            )
        )
        pbar.set_description(
            f"Processing {year} - ({len(n_filt):,} Nodes, {len(e_filt):,} Edges, {len(ind_filt):,} Indications)"
        )
        pbar.refresh()

        # Save the network, indications, and summary figure
        n_filt.write_parquet(file=os.path.join(out_dir, "nodes.parquet"))
        e_filt.write_parquet(file=os.path.join(out_dir, "edges.parquet"))
        ind_filt.write_parquet(file=os.path.join(out_dir, "indications.parquet"))

    logger.info("Done running 06_Resolve_Network_Edges_by_Time.py\n")


if __name__ == "__main__":
    main(parse_args())
