import os
import pickle
import warnings

import polars as pl
from tqdm import tqdm

warnings.filterwarnings("ignore")
import argparse


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Split dataset by time. 
        Use this script to split node/edge SemMedDB files to train/test or train/test/valid with or without years column.""",
        usage="07_Build_data_split.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-b",
        "--base_dir",
        default="../data/time_networks-6_metanode",
        type=str,
        help="directory to build the time-based dataset",
    )
    parser.add_argument(
        "-n",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )
    parser.add_argument(
        "-p",
        "--split_hyperparameter_optimization",
        default=False,
        action="store_true",
        help="create a hyperparameter optimization split, where the train triples are equal to or less than the given year, and test/valid triples are greater than the given year",
    )

    parser.add_argument(
        "-s",
        "--split_train_test_valid",
        default=False,
        action="store_true",
        help="split data into train/test/valid sets, where train/test triples are equal to or less than the given year, and valid triples are greater than the given year. If 'False', only split into train and test, where train triples are equal to or less than the given year, and test triples are greater than the given year.",
    )
    parser.add_argument(
        "-t",
        "--include_time",
        default=False,
        action="store_true",
        help="include time in the dataset",
    )
    parser.add_argument(
        "-y",
        "--hpo_year",
        default="1987",
        type=str,
        help="Year to split the hyperparameter optimization dataset. Default is 1987. Only used if split_hyperparameter_optimization is True.",
    )

    return parser.parse_args(args)


def main(args):
    print("Running 07_Build_data_split.py")
    print(f"--split_train_test_valid: {args.split_train_test_valid}")
    print(
        f"--split_hyperparameter_optimization: {args.split_hyperparameter_optimization}"
    )
    print(f"--include_time: {args.include_time}")
    print(f"changing directory to {args.base_dir}")
    os.chdir(args.base_dir)

    print(
        f"... splitting edges dataset up into train/test/validation, where train/test are triples in the 'present/past' and validation is 'future' of a given time point"
    )

    # Train/Test/Validation Split, where Train and Test are Present and Past, and Validation is Future
    for file in (
        pbar := tqdm(
            [f for f in os.listdir() if (f.startswith("19") or f.startswith("20"))]
        )
    ):

        pbar.set_description(f"... creating train/test/valid split - {file}")

        if args.include_time:
            time_txt = "time"
        else:
            time_txt = "notime"

        if args.split_train_test_valid:

            train, test, valid = make_train_test_valid_set(file, time=args.include_time)
            train.write_csv(
                file=os.path.join(file, f"train_ttv_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
            test.write_csv(
                file=os.path.join(file, f"test_ttv_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
            valid.write_csv(
                file=os.path.join(file, f"valid_ttv_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
        else:  # create train/test if train/test/valid is false

            train, test = make_train_test_set(file, time=args.include_time)
            train.write_csv(
                file=os.path.join(file, f"train_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
            test.write_csv(
                file=os.path.join(file, f"test_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )

        if args.split_hyperparameter_optimization and file == args.hpo_year:

            train, test, valid = make_hpo_split(file, time=args.include_time)
            train.write_csv(
                file=os.path.join(file, f"hpo_train_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
            test.write_csv(
                file=os.path.join(file, f"hpo_test_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )
            valid.write_csv(
                file=os.path.join(file, f"hpo_valid_{time_txt}.txt"),
                separator="\t",
                include_header=False,
            )

    print("Done running 07_Build_data_split.py\n")


def read_dataframes(file_dir: str) -> pl.DataFrame:
    # check if files exist
    for file in [
        (nodes_loc := os.path.join(file_dir, "nodes.parquet")),
        (edges_loc := os.path.join(file_dir, "edges.parquet")),
        (indic_loc := os.path.join(file_dir, "indications.parquet")),
    ]:
        assert os.path.exists(file) == True, f"{file} not in given file directory"

    # read files
    nodes = pl.read_parquet(source=nodes_loc)
    edges = pl.read_parquet(source=edges_loc)
    indications = pl.read_parquet(
        source=indic_loc,
        columns=[
            "compound_semmed_id",
            "disease_semmed_id",
            "approval_date",
            "approval_year",
            "year_diff",
            "year_cat",
        ],
    )

    return nodes, edges, indications


def get_node_types(nodes: pl.DataFrame, edges: pl.DataFrame) -> pl.DataFrame:
    """
    Adds non-abbreviated and abbreviated node types to edges file as 'htype' and 'ttype' and 'h_abv' and 't_abv', respectively.
    """
    node_dict = dict(zip(nodes["id"], nodes["label"]))
    node_short_dict = dict(zip(nodes["id"], nodes["abv_label"]))
    edges = edges.with_columns(
        pl.col("h_id").replace(node_dict).alias("htype"),
        pl.col("t_id").replace(node_dict).alias("ttype"),
        pl.col("h_id").replace(node_short_dict).alias("h_abv"),
        pl.col("t_id").replace(node_short_dict).alias("t_abv"),
    )

    return edges


def create_acronym_dict(edges: pl.DataFrame) -> dict:
    """
    Given edges dataframe, extract relations, htype and ttype to create a dictionary of label abbreviations
    """
    # check for appropriate columns
    assert (
        "htype" in edges.columns
        or "ttype" not in edges.columns
        or "h_abv" not in edges.columns
        or "t_abv" not in edges.columns
    ), "Node types not in edges. Use get_node_types() to add them"

    rel_dict = dict(
        zip(edges["htype"], edges["h_abv"])
    )  # Get all head nodes and their abbreviations
    rel_dict.update(
        dict(zip(edges["ttype"], edges["t_abv"]))
    )  # Get all tail nodes and their abbreviations
    rel_dict.update(
        dict(zip(edges["sem"], edges["rtype"]))
    )  # Get all relations and their abbreviations
    return rel_dict


def get_indication_types(file_dir: str) -> pl.DataFrame:
    """
    Inidication file does not have relations with metatype information, and not all are Drugs->Diseases.
    This function adds a column to the indication dataframe with typed relations
    """
    # imports files
    nodes, edges, indications = read_dataframes(file_dir)

    # get node types added to edge file
    edges = get_node_types(nodes=nodes, edges=edges)

    # build a dictionary from the nodes file if it doesn't exist, otherwise import mappings
    dir_loc = os.path.join(file_dir, "./abbv_dict.pkl")
    if os.path.exists(dir_loc) == False:
        abbv_dict = create_acronym_dict(edges)
        with open(dir_loc, "wb") as f:
            pickle.dump(abbv_dict, f)
    else:
        with open(dir_loc, "rb") as f:
            abbv_dict = pickle.load(f)

    abbv_dict["INDICATION"] = "i"

    # create relation, 'r'
    indications = indications.rename(
        {"compound_semmed_id": "h_id", "disease_semmed_id": "t_id"}
    )
    indications = get_node_types(nodes=nodes, edges=indications)

    indications = indications.with_columns(
        r=pl.struct(["htype", "ttype"]).map_elements(
            lambda x: "INDICATION_"
            + abbv_dict[x["htype"]]
            + abbv_dict["INDICATION"]
            + abbv_dict[x["ttype"]]
        )
    )

    return nodes, edges, indications


def make_train_test_set(file_dir: str, time: bool = True) -> pl.DataFrame:
    """
    Given a polars dataframe of edges and indications, return a training set.
    If time is set to False, removes time column from exported dataframe. Training
    set comes from edges prior-to the file time. Testing set comes from edges after
    the file time. No 80/10/10 split.
    """

    # read in nodes, edges, indications and append node types to edge
    nodes, edges, _ = read_dataframes(file_dir)
    edges = get_node_types(nodes=nodes, edges=edges)

    # process to get indications file and remove weird edge types
    _, _, indications = get_indication_types(file_dir)
    indications = indications.filter(pl.col("r") != "INDICATION_CDiCD")

    edges = edges[["h_id", "r", "t_id", "first_pub"]]
    indications_train = indications.filter(pl.col("year_diff") <= 0)[
        ["h_id", "r", "t_id", "approval_year"]
    ]
    indications_test = indications.filter(pl.col("year_diff") > 0)[
        ["h_id", "r", "t_id", "approval_year"]
    ]

    # rename columns from x to year
    edges = edges.rename({"first_pub": "year"}).with_columns(pl.col("year").cast(str))
    indications_train = indications_train.rename(
        {"approval_year": "year"}
    ).with_columns(pl.col("year").cast(str))
    indications_test = indications_test.rename({"approval_year": "year"}).with_columns(
        pl.col("year").cast(str)
    )

    train = edges.vstack(indications_train)

    if time == False:
        train = train[["h_id", "r", "t_id"]]
        indications_test = indications_test[["h_id", "r", "t_id"]]

    return train, indications_test


def make_train_test_valid_set(file_dir: str, time: bool = True) -> pl.DataFrame:
    """
    Given a polars dataframe of edges and indications, return a training/testing/validation set,
    where training and testing are derived from prior to the edge 'year' specified by the folder name,
    and validation set is derived from after the edge 'year' specified by folder name

    :file_dir:               - a directory to the file of nodes, edges, and indications.csv. i.e "./1950"
    """

    # read in nodes, edges, indications and append node type to edge
    nodes, edges, _ = read_dataframes(file_dir)
    edges = get_node_types(nodes=nodes, edges=edges)

    # process the indication file and remove weird edge types
    _, _, indications = get_indication_types(file_dir)
    indications = indications.filter(
        pl.col("r") != "INDICATION_CDiCD"
    )  # ~ means inverse is_in
    indications = indications.rename({"approval_year": "year"})

    edges = edges[["h_id", "r", "t_id", "first_pub"]]
    indications_train_test = indications.filter(pl.col("year_diff") <= 0)[
        ["h_id", "r", "t_id", "year"]
    ].with_columns(pl.col("year").cast(str))
    indications_validation = indications.filter(pl.col("year_diff") > 0)[
        ["h_id", "r", "t_id", "year"]
    ].with_columns(pl.col("year").cast(str))

    # rename columns from 'x' to year
    edges = edges.rename({"first_pub": "year"}).with_columns(pl.col("year").cast(str))

    # split indications_train_test to 80% train, 20% test
    indications_train = indications_train_test.sample(
        fraction=0.8, with_replacement=False, seed=12345
    )
    indications_test = indications_train_test.join(
        indications_train, on=["h_id", "r", "t_id", "year"], how="anti"
    )

    train = edges.vstack(indications_train)

    if time == False:
        train = train[["h_id", "r", "t_id"]]
        indications_test = indications_test[["h_id", "r", "t_id"]]
        indications_validation = indications_validation[["h_id", "r", "t_id"]]

    return train, indications_test, indications_validation


# make a hyper parameter optimization dataset by splitting up the train set into a HPO test/valid.
# HPO will be on 1987
def make_hpo_split(file_dir: str, time: bool = True) -> pl.DataFrame:
    """
    Create a train/test/valid dataset for hyperparameter optimizations.
    """
    # import relevant files
    train, test = make_train_test_set(file_dir, time)

    # get training indications only
    train_ind = train.filter(
        pl.col("r").is_in(["INDICATION_CDiDO", "INDICATION_CDiPS"])
    )
    # remove training indications from train
    train_no_ind = train.join(train_ind, on=["h_id", "r", "t_id"], how="anti")

    # split train_ind into train and eval
    hpo_train = train_ind.sample(fraction=0.8, seed=12345)
    hpo_test = train_ind.join(hpo_train, on=["h_id", "r", "t_id"], how="anti").sample(
        fraction=0.5, seed=67890
    )

    # get remaining eval and assign to validation
    hpo_valid = train_ind.join(hpo_train, on=["h_id", "r", "t_id"], how="anti").join(
        hpo_test, on=["h_id", "r", "t_id"], how="anti"
    )

    # put non-indications together with 80% indication split
    hpo_train = train_no_ind.vstack(hpo_train)

    return hpo_train, hpo_test, hpo_valid


if __name__ == "__main__":
    main(parse_args())
