import os
import pathlib
from collections import ChainMap
from typing import List, Optional, Tuple

import polars as pl
import pykeen
from pykeen.datasets.timeresolvedkg import TimeResolvedKG as trkg
from pykeen.pipeline import pipeline
from pykeen.predict import predict_all


class TimeDrugRepo(object):
    """
    Class for identifying optimal years to train on to maximize downstream sample size, and has a wrapper to initiate `pykeen.pipeline.pipeline` should you have provided adequate kwargs to run the model. Reference ./031_maximize_samples_from_years2.ipynb for more details on how each function was built.

    Parameters
    ----------
    :param: data_dir (str):    The path to the time-resolved dataset folder. Ex. "../data/time_networks-6_metanode/"
    :param: models_to_run (int):    The number of models to train. Ex. "models_to_run = 4" will pick the four years with the most indications
    :param: train_models (bool):    Whether to train models on the recommended year dataset. If you supply pykeen kwargs for `pykeen.pipeline.pipeline`, True will train the recommended year models, Otherwise it will fail

    Properties
    ----------
    self.recommended_years ([int]):    List of years to run
    self.recommneded_ind_counts (int):    total number of indications to evaluate
    self.recommended_df (pl.DataFrame):     group-by dataframe by years with specific drug-dis indications
    """

    def __init__(
        self,
        data_dir: str = "../data/time_networks-6_metanode/",
        models_to_run: int = 5,
        train_models: bool = False,
        train_models_swap: bool = False,
        steps: int = 1511121,  #  the number of triples in the 1987 dataset. Use this normalize batch size to dataset size
        build_dataset_kwargs: dict = {},
        **kwargs,
    ):
        self.data_dir = data_dir
        self.years = [  # automatically should throw an error if an item in the directory cannot be converted to an integer
            int(i)
            for i in os.listdir(self.data_dir)
            if os.path.isdir(os.path.join(self.data_dir, i))
        ]

        self.models_to_run = models_to_run
        self.train_models = train_models
        self.train_models_swap = train_models_swap
        self.steps = steps
        self.build_dataset_kwargs = build_dataset_kwargs
        # self.train, self.test, self.ind = self.import_df()
        # self.node_time_dictionary = self.create_node_time_dict()

        self.recommended_years = self.recommend()
        self.recommended_df = self.recommend_compound_disease_pair()
        self.recommended_counts = self.recommended_indication_counts()

        if self.train_models:
            self.run_model(**kwargs)

        if self.train_models_swap:
            self.run_model_swap_test_valid(**kwargs)

    def read_df(self, year: int, filename: str) -> pl.DataFrame:
        """
        Get a dataframe for a given year and name
        """
        assert filename in [
            "train",
            "test",
            "valid",
        ], "filename must be either 'train', 'test', or 'valid'"

        file_dir = pathlib.Path(self.data_dir)
        # parameters for file string
        ttv = (
            "_ttv" if self.build_dataset_kwargs.get("split_ttv", False) != False else ""
        )
        time = (
            "_time"
            if self.build_dataset_kwargs.get("include_time", False) != False
            else "_notime"
        )
        headers = ["h", "r", "t", "year"] if time == "_time" else ["h", "r", "t"]

        # new file path
        new_file_dir = pathlib.Path(file_dir, str(year), f"{filename}{ttv}{time}.txt")
        df = pl.read_csv(new_file_dir, separator="\t", new_columns=headers)
        return df

    def get_indication_count(self, year: int) -> Tuple[int, int, Optional[int]]:
        """Query train, test, and validation indication counts for a given year"""

        train_count = (
            self.read_df(year=year, filename="train")
            .filter(pl.col("r") == "INDICATION_CDiDO")
            .unique()
            .shape[0]
        )

        test_count = (
            self.read_df(year=year, filename="test")
            .filter(pl.col("r") == "INDICATION_CDiDO")
            .unique()
            .shape[0]
        )

        if self.build_dataset_kwargs.get("split_ttv", False) != False:
            valid_count = (
                self.read_df(year=year, filename="valid")
                .filter(pl.col("r") == "INDICATION_CDiDO")
                .unique()
                .shape[0]
            )

            return train_count, test_count, valid_count

        else:
            return train_count, test_count

    def get_indication_counts_df(self) -> pl.DataFrame:
        """
        Generates a dataframe with the train, test, valid indication counts for each year in the dataset
        """
        years = self.years

        train_counts, test_counts, valid_counts = list(), list(), list()
        # don't include 2022 and 2023 for years
        for year in years[:-3]:
            if self.build_dataset_kwargs.get("split_ttv", False) != False:
                train, test, valid = self.get_indication_count(year)
                valid_counts.append(valid)
            else:
                train, test = self.get_indication_count(year)

            train_counts.append(train)
            test_counts.append(test)

        # create dataframe of indication counts
        df = pl.DataFrame(
            {
                "year": years[:-3],
                "train_indications": train_counts,
                "test_indications": test_counts,
                "valid_indications": valid_counts,
            }
        )

        return df

    def get_all_indications_df(self) -> pl.DataFrame:
        """
        Iterates through all years in the dataset and returns a dataframe with all indications
        """
        years = self.years
        all_indications = list()

        for year in years[:-3]:
            df = self.read_df(year=year, filename="valid")
            df = df.with_columns(ds_year=pl.lit(year))
            all_indications.append(df)

        all_indications_df = pl.concat(all_indications)
        return all_indications_df

    def recommend(self) -> Tuple[List[int], int, pl.DataFrame]:
        """
        Recommend the subset of years to train on based on `self.models_to_run`
        """
        self.ind_counts_df = self.get_indication_counts_df()
        ind_counts_df = self.ind_counts_df

        ind_counts_df = ind_counts_df.with_columns(
            test_valid=pl.col("test_indications") + pl.col("valid_indications")
        ).sort("test_valid", descending=True)

        recommended_years = list()
        recommended_years.append(ind_counts_df["year"].gather(0).item())

        self.all_inds_df = self.get_all_indications_df()
        while len(recommended_years) < self.models_to_run:
            # get already recommended year triples to maximize the ones that haven't been recommended
            already_recommended = self.all_inds_df.filter(
                pl.col("ds_year").is_in(recommended_years)
            )
            # get the year with the most indications, sans the ones already recommended
            rec_year = (
                self.all_inds_df.join(already_recommended, on=["h", "t"], how="left")
                .filter(pl.col("r_right").is_null())
                .group_by("ds_year")
                .agg(pl.count("h").alias("count"))
                .sort("count", descending=True)["ds_year"]
                .gather(0)
                .item()
            )

            recommended_years.append(rec_year)

        return recommended_years

    def recommend_compound_disease_pair(self) -> pl.DataFrame:
        """
        Takes a recommended list of years and returns a dataframe of the compound-disease pairs for each year.
        Can use this to check if the recommendations are IID.
        """
        all_inds_df = self.all_inds_df
        recommended_cd_pairs = all_inds_df.filter(
            pl.col("ds_year").is_in(self.recommended_years)
        )
        return recommended_cd_pairs

    def recommended_indication_counts(self) -> pl.DataFrame:
        """
        Returns a dataframe of the recommended years and their indication counts
        """

        return self.ind_counts_df.filter(pl.col("year").is_in(self.recommended_years))

    def run_model(self, **kwargs):
        """
        Wrapper function for pykeen.pipeline.pipeline that loops over all years in self.years_ls in the dataset
        """
        print(f"Running Models for the following years {self.recommended_years}")
        for year in self.recommended_years:
            # replace file name everytime you run the algorithm, so no collisions
            year = str(year)
            checkpoint_file_name = f"{kwargs['training_kwargs']['checkpoint_name'].split('.')[0]}_{year}.pt"

            # get training triples by loading them from trkg
            dataset = trkg(build_dataset_kwargs=self.build_dataset_kwargs, year=year)
            train_sz = dataset.training.num_triples

            batch_size = (
                kwargs["training_kwargs"]["batch_size"] * train_sz
            ) // self.steps
            new_kwargs = ChainMap(
                {
                    "training_kwargs": ChainMap(
                        {
                            "checkpoint_name": checkpoint_file_name,
                            "batch_size": batch_size,  # change num epochs with reference to batch sizes so all steps are the same
                        },
                        kwargs["training_kwargs"],
                    )
                },
                kwargs,
            )

            print(f"Checkpoint name: {checkpoint_file_name}")

            res = pipeline(
                dataset=dataset,
                # training=os.path.join(self.data_dir, str(year), "train_notime.txt"),
                # testing=os.path.join(self.data_dir, str(year), "test_notime.txt"),
                **new_kwargs,
            )

    def run_model_swap_test_valid(self, **kwargs):
        """
        Wrapper function for pykeen.pipeline.pipeline that loops over all years in self.years_ls in the dataset
        """
        print(f"Running Models for the following years {self.recommended_years}")
        for year in self.recommended_years:
            # replace file name everytime you run the algorithm, so no collisions
            year = str(year)
            checkpoint_file_name = f"{kwargs['training_kwargs']['checkpoint_name'].split('.')[0]}_{year}.pt"

            # get training triples by loading them from trkg
            dataset = trkg(build_dataset_kwargs=self.build_dataset_kwargs, year=year)
            train_sz = dataset.training.num_triples

            batch_size = (
                kwargs["training_kwargs"]["batch_size"] * train_sz
            ) // self.steps
            new_kwargs = ChainMap(
                {
                    "training_kwargs": ChainMap(
                        {
                            "checkpoint_name": checkpoint_file_name,
                            "batch_size": batch_size,  # change num epochs with reference to batch sizes so all steps are the same
                        },
                        kwargs["training_kwargs"],
                    )
                },
                kwargs,
            )

            print(f"Checkpoint name: {checkpoint_file_name}")

            res = pipeline(
                training=dataset.training_path,
                testing=dataset.validation_path,
                validation=dataset.testing_path,
                **new_kwargs,
            )


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


def get_time(df: pl.DataFrame, file_dir: str) -> pl.DataFrame:
    """
    Get the time of each triple in the dataset and append to train/test/indication dataframes
    """
    _, edges, indications = get_indication_types(file_dir)

    combined_df = pl.concat(
        [
            edges.select(["h_id", "r", "t_id", "first_pub"])
            .rename({"first_pub": "year"})
            .with_columns(pl.col("year").cast(str)),
            indications.select(["h_id", "r", "t_id", "approval_year"]).rename(
                {"approval_year": "year"}
            ),
        ]
    ).unique()

    df = df.join(
        combined_df, left_on=["h", "r", "t"], right_on=["h_id", "r", "t_id"], how="left"
    )

    return df


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
    abbv_dict = create_acronym_dict(edges)
    # dir_loc = os.path.join(file_dir, "./abbv_dict.pkl")
    # if os.path.exists(dir_loc) == False:
    #     abbv_dict = create_acronym_dict(edges)
    #     with open(dir_loc, "wb") as f:
    #         pickle.dump(abbv_dict, f)
    # else:
    #     with open(dir_loc, "rb") as f:
    #         abbv_dict = pickle.load(f)

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
