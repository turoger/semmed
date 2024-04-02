import os
import pickle
from collections import ChainMap

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
        steps: int = 1511121,
        build_dataset_kwargs: dict = {},
        **kwargs,
    ):
        self.data_dir = data_dir
        self.models_to_run = models_to_run
        self.train_models = train_models
        self.steps = steps
        self.build_dataset_kwargs = build_dataset_kwargs
        self.train, self.test, self.ind = self.import_df()
        self.node_time_dictionary = self.create_node_time_dict()
        (
            self.recommended_years,
            self.recommended_ind_counts,
            self.recommended_df,
        ) = self.recommend()

        if self.train_models:
            self.run_model(**kwargs)

    def import_df(self) -> pl.DataFrame:
        """
        import and build your train/test/indication data
        ----------
        :param: dir :    string pointing directory holding all time folders
        """
        # directories
        dir = self.data_dir
        time = (
            "time" if self.build_dataset_kwargs.get("include_time", False) else "notime"
        )
        headers = ["h", "r", "t", "year"] if time == "time" else ["h", "r", "t"]
        file_dir = os.path.join(dir, "2023")
        train_dir = os.path.join(file_dir, f"train_{time}.txt")
        ind_dir = os.path.join(file_dir, "indications.parquet")

        # import train
        train = pl.read_csv(
            source=train_dir,
            has_header=False,
            new_columns=headers,
            separator="\t",
        )

        # import ind
        ind = pl.read_parquet(source=ind_dir)
        ind = ind.with_columns(
            pl.col("approval_year").str.split("-").list.first().alias("year")
        ).drop_nulls()  # get indication year

        # import test
        test_imports = [
            pl.read_csv(
                source=os.path.join(dir, i, f"test_{time}.txt"),
                new_columns=headers,
                separator="\t",
                raise_if_empty=False,
            )
            for i in os.listdir(dir)
            if os.path.isdir(os.path.join(dir, i)) & (i not in ["2022", "2023"])
        ]  # import all tests into a list. Don't import 22' and 23' because tests are empty

        test = pl.concat(
            test_imports
        ).unique()  # concatenate all tests and remove duplicates

        if time == "notime":
            # get date for train and test

            train = get_time(train, file_dir).drop_nulls()
            test = get_time(test, file_dir).drop_nulls()

        return train, test, ind

    def create_node_time_dict(self) -> dict:
        """
        builds node-time dictionary
        ----------
        :param: train :    train file to solicit identifier to year links
        """
        train, test, ind = self.train, self.test, self.ind

        # combine train and test nodes together and get the min year of each node

        node_time = (
            pl.concat(
                [
                    train.select(["h", "year"]),
                    train.select(["t", "year"]).rename({"t": "h"}),
                    test.select(["h", "year"]),
                    test.select(["t", "year"]).rename({"t": "h"}),
                ]
            )
            .unique()
            .group_by("h")
            .agg("year")
            .with_columns(
                pl.col("year").list.sort(descending=False),
                pl.col("year").list.min().alias("min_year"),
            )  # group by nodes to get [time] sorted from low to high (1950->2023)
            .drop_nulls()
        )

        # construct dictionary
        node_time_dict = dict(
            zip(node_time["h"], [str(i) for i in node_time["min_year"]])
        )

        return node_time_dict

    def recommend(self) -> tuple:
        """
        Recommend the model years to train on based on specified model counts
        ----------
        :param: train :    training file from self
        :param: test :    testing file from self
        :param: ind :    indication file from self
        :param: combo_ct :    numberumber of models to train from self
        """
        train, test, ind = self.train, self.test, self.ind
        node_time_dict = self.node_time_dictionary
        combo_ct = self.models_to_run

        # create new indications file
        ind2 = ind[["compound_semmed_id", "disease_semmed_id", "year"]].filter(
            pl.col("compound_semmed_id").is_in(node_time_dict.keys()),
            pl.col("disease_semmed_id").is_in(node_time_dict.keys()),
        )

        ind2 = (
            ind2.with_columns(  # create two new columns based on entity->yr mapping
                pl.col("compound_semmed_id")
                .replace(node_time_dict)
                .alias("comp_year")
                .str.to_integer(),
                pl.col("disease_semmed_id")
                .replace(node_time_dict)
                .alias("dis_year")
                .str.to_integer(),
            )
            .with_columns(  # create column that picks the younger from columns comp year & dis year
                younger_year=(
                    pl.when(pl.col("comp_year") >= pl.col("dis_year"))
                    .then(pl.col("comp_year"))
                    .otherwise(pl.col("dis_year"))
                )
            )
            .with_columns(  # create column that takes the difference between approval year and appearance of entity in KG
                approval_younger_year_diff=(
                    pl.col("year").cast(pl.Int64)
                    - pl.col("younger_year").cast(pl.Int64)
                ),
            )
        )
        # create a filtered group_by dataframe to get total indications associated with years
        indcounts = (
            ind2.filter(
                pl.col("compound_semmed_id").is_in(
                    test["h"]
                ),  # make sure its in the test set
                pl.col("disease_semmed_id").is_in(
                    test["t"]
                ),  # make sure its in the test set
                pl.col("approval_younger_year_diff")
                > 0,  # predict the future, not the current
                pl.col("year").cast(pl.Int64) > 1950,  # dataset lower bound
                pl.col("younger_year").cast(pl.Int64) >= 1950,  # dataset lower bound
            )
            .group_by("younger_year")
            .agg(
                ["year", "compound_semmed_id", "disease_semmed_id"]
            )  # column lists to return
            .with_columns(pl.col("year").list.len().alias("counts"))  # ind counts/yr
            .sort(by="counts", descending=True)
        )

        # results
        picked_years = indcounts["younger_year"][0:combo_ct].to_list()
        totals = indcounts["counts"][0:combo_ct].sum()

        return picked_years, totals, indcounts

    def run_model(self, **kwargs):
        """
        Wrapper function for pykeen.pipeline.pipeline that loops over all years in self.years_ls in the dataset
        """
        print(f"Running Models for the following years {self.recommended_years}")
        for year in self.recommended_years:
            # replace file name everytime you run the algorithm, so no collisions
            checkpoint_file_name = f"{kwargs['training_kwargs']['checkpoint_name'].split('.')[0]}_{year}.pt"

            # get training triples by loading them from trkg
            train_sz = trkg(build_dataset_kwargs={}, year=year).training.num_triples

            batch_size = (
                kwargs["training_kwargs"]["batch_size"] * train_sz
            ) // self.steps
            new_kwargs = ChainMap(
                {
                    "training_kwargs": ChainMap(
                        {
                            "checkpoint_name": checkpoint_file_name,
                            "batch_size": batch_size[
                                0
                            ],  # change num epochs with reference to batch sizes so all steps are the same
                        },
                        kwargs["training_kwargs"],
                    )
                },
                kwargs,
            )

            print(f"Checkpoint name: {checkpoint_file_name}")

            res = pipeline(
                dataset=trkg(year=f"{year}")
                # training=os.path.join(self.data_dir, str(year), "train_notime.txt"),
                # testing=os.path.join(self.data_dir, str(year), "test_notime.txt"),
                ** new_kwargs,
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
