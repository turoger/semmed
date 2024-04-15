import pathlib
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
    cast,
)

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import pykeen
import pykeen.constants
import pykeen.datasets.timeresolvedkg as trkg
import pykeen.models
import seaborn as sns
import torch
from pykeen.predict import predict_all, predict_target

### Helper functions for analysing pykeen outputs

### Available functions
#
# load_pykeen_triples() ->      Imports PyKEEN triples factory based on build parameter and year subset.
# predict_on() ->               Extracts predictions for 'test' or 'valid' sets given a pykeen model
# get_answers() ->              Calculates a n-hot encoded list for the answers of a given query (head/tail)
# get_rank() ->                 Returns the filtered rank
# get_mrr() ->                  Returns the MRR score
# get_hits_at_k() ->            Returns the number of hits at k
# extract_answers_from_rank() -> Extracts answers from rank dataframe
# extract_relative_year() ->    Extracts relative year from indications
#
# static methods
# load_pykeen_dataset() ->      Imports PyKEEN dataset TRKG based on build parameter and year subset.
# get_rank_from_position() ->   Gets positions of all hits and returns as an array. Accounts for ties, rank starts at 1.
# get_true_index() ->           Gets the index of all hits and returns as as an array.


class AnalysisHelper(object):
    """
    Get the predictions, true answer rank, hits_at_k, and MRR performance of a given model

    Parameters
    -----------
    :build_dataset_kwargs: (dict)   parameters supplied to build the knolwedge graph
    :pykeen_model:                  pykeen model used to make predictions
    :chkpt_file: (str)              pykeen model checkpoint file name
    :group: (str)                   evaluate on 'test' or 'valid'?
    :qa: (str)                      short for query_answer; predict on 'head', 'tail', or 'both'?
    :cuda: (bool)                   use gpu or cpu?
    """

    def __init__(
        self,
        build_dataset_kwargs: Optional[Mapping[str, Any]] = {},
        year: str = "1950",
        train_models_swap: Optional[bool] = False,
        pykeen_model: Optional[pykeen.models.base.Model] = None,
        chkpt_file: Optional[str] = "TransE_neg_1950.pt",
        group: Optional[str] = "test",  # {'test','valid'},
        qa: Optional[str] = "both",  # {'both','head','tail'}
        cuda: Optional[bool] = False,
    ):
        self.build_dataset_kwargs = build_dataset_kwargs
        self.year = year
        self.train_models_swap = train_models_swap
        self.dataset = self.load_pykeen_dataset(
            build_dataset_kwargs=self.build_dataset_kwargs, year=self.year
        )
        self.group = group
        self.query_answer = qa
        self.pykeen_model = pykeen_model
        # self.model_kwargs = model_kwargs
        self.chkpt_dir = pykeen.constants.PYKEEN_CHECKPOINTS.joinpath(chkpt_file)

        # check if query_answer is valid
        if self.query_answer != None:
            assert self.query_answer in [
                "head",
                "tail",
                "both",
            ], "query_answer must be one of ['head','tail','both']"

        # check if group is valid
        if self.group != None:
            assert self.query_answer in [
                "test",
                "valid",
                "both",
            ], "query_answer must be one of ['test','valid','both']"

        # load train/test/valid if ttv is true otherwise just train/test
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            self.train, self.test, self.valid = self.load_pykeen_triples()
        else:
            self.train, self.test = self.load_pykeen_triples()

        # load pykeen model if model and checkpoint available
        if self.pykeen_model != None:
            # check if checkpoint exists
            assert (
                self.chkpt_dir.is_file()
            ), f"Model checkpoint not found at `{self.chkpt_dir}`"

            # create df from model predictions
            if cuda:
                # load checkpoint in torch
                self.chkpt = torch.load(
                    self.chkpt_dir, map_location=torch.device("cuda:0")
                )
                # load checkpoint in to model
                self.pykeen_model.load_state_dict(self.chkpt["model_state_dict"])
                # make predictions
                self.df = self.predict_on(pykeen_model=self.pykeen_model.cuda(0))
            else:
                self.chkpt = torch.load(
                    self.chkpt_dir, map_location=torch.device("cpu")
                )
                self.pykeen_model.load_state_dict(self.chkpt["model_state_dict"])
                self.df = self.predict_on(pykeen_model=self.pykeen_model)

            self.df = (
                self.get_answers()
            )  # returns a list of [1,0] where 1 denotes answer index
            self.df = (
                self.get_rank()
            )  # returns a list of filtered ranks for each answer

            self.answer_df = (
                self.extract_answers_from_rank()
            )  # returns expanded list of only answers
            self.answer_df = (
                self.extract_relative_year()
            )  # returns year for rexpanded list of only answers

            self.mrr = self.get_mrr()
            self.hits_1 = self.get_hits_at_k(k=1)
            self.hits_3 = self.get_hits_at_k(k=3)
            self.hits_10 = self.get_hits_at_k(k=10)
            self.hits_100 = self.get_hits_at_k(k=100)

    @staticmethod
    def load_pykeen_dataset(
        build_dataset_kwargs: Mapping[str, any], year: str
    ) -> "TimeResolvedKG":
        """
        Takes parameters used to build the dataset and a given year to import PykEEN dataset class
        """

        return trkg.TimeResolvedKG(build_dataset_kwargs=build_dataset_kwargs, year=year)

    def load_pykeen_triples(
        self,
    ) -> Tuple["TriplesFactory", "TriplesFactory", Optional["TriplesFactory"]]:
        """
        Takes parameters used to build the dataset and a given year to import PykEEN triplesfactory
        """
        the_dataset = self.load_pykeen_dataset(
            build_dataset_kwargs=self.build_dataset_kwargs, year=self.year
        )

        train = the_dataset.training
        test = the_dataset.testing

        # return train, test, valid if split_ttv is not 0
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            # swap test and valid if models_swap is True
            if self.train_models_swap:
                test = the_dataset.validation
                valid = the_dataset.testing
                self.test_dir = the_dataset.validation_path
                self.valid_dir = the_dataset.testing_path
            else:
                test = the_dataset.testing
                valid = the_dataset.validation
                self.test_dir = the_dataset.testing_path
                self.valid_dir = the_dataset.validation_path

            return train, test, valid

        else:
            return train, test

    def predict_on(
        self,
        pykeen_model: pykeen.models.base.Model,
        query_answer: Optional[str] = "both",  # {both,tail,head}
    ) -> pl.DataFrame:
        """
        Takes a PyKEEN model and testing path to generate all compound indications on test set.
        group can be 'test' or 'valid' splits depending on the group to be assessed
        query_answer can be 'both', 'tail', 'head'. 'both' returns both head and tail predictions, 'tail' returns only tail predictions, 'head' returns only head predictions
        """
        query_answer = self.query_answer
        group = self.group

        # setup train, test, valid if there is a validation set available
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            train, _, _ = self.load_pykeen_triples()
        else:
            assert (
                group == "test"
            ), "No validation set found, in `self.build_dataset_kwargs` please use test group"
            train, _ = self.load_pykeen_triples()

        # import test or valid triples depending on 'group'
        predict_path = (
            self.dataset.testing_path
            if group == "test"
            else self.dataset.validation_path
        )

        # DataFrame for predictions
        predict_df = pd.read_csv(predict_path, sep="\t", names=["h", "r", "t"])

        # build a dictionary for each condition
        query_answer_dict = {
            "head": dict(zip(predict_df["t"], predict_df["r"])),
            "tail": dict(zip(predict_df["h"], predict_df["r"])),
        }

        if query_answer == "head" or query_answer == "both":
            h_res_ls = list()
            for disease, relation in query_answer_dict["head"].items():
                pred = pykeen.predict.predict_target(
                    model=pykeen_model,
                    tail=disease,
                    relation=relation,
                    triples_factory=train,
                )
                # .add_membership_columns(
                #     testing=test,
                #     validation=(
                #         valid
                #         if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                #         else None
                #     ),
                # )  # check if tail predictions are in 'predict_triple' (correct answer)
                pred = pl.DataFrame(pred.df)
                pred = pred.with_columns(t=pl.lit(disease))
                h_res_ls.append(pred)
            df_h = pl.concat(h_res_ls)

        if query_answer == "tail" or query_answer == "both":
            t_res_ls = list()
            for compound, relation in query_answer_dict["tail"].items():
                pred = pykeen.predict.predict_target(
                    model=pykeen_model,
                    head=compound,
                    relation=relation,
                    triples_factory=train,
                )
                # .add_membership_columns(
                #     testing=test,
                #     validation=(
                #         valid
                #         if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                #         else None
                #     ),
                # )  # check if tail predictions are in 'predict_triple' (correct answer)
                pred = pl.DataFrame(pred.df)
                pred = pred.with_columns(h=pl.lit(compound))
                t_res_ls.append(pred)
            df_t = pl.concat(t_res_ls)

        if query_answer == "head":
            df_h = (
                df_h.with_columns(query=pl.lit("head"))
                .rename({"t": "query_label", "head_label": "answer_label"})
                .drop("head_id")
                .group_by(["query_label", "query"], maintain_order=True)
                .agg(
                    [
                        "answer_label",
                    ]
                )
            )

            return df_h

        elif query_answer == "tail":
            df_t = (
                df_t.with_columns(query=pl.lit("tail"))
                .rename({"h": "query_label", "tail_label": "answer_label"})
                .drop("tail_id")
                .group_by(["query_label", "query"], maintain_order=True)
                .agg(
                    [
                        "answer_label",
                    ]
                )
            )

            return df_t

        else:
            df_h = (
                df_h.with_columns(query=pl.lit("head"))
                .rename({"t": "query_label", "head_label": "answer_label"})
                .drop("head_id")
            )
            df_t = (
                df_t.with_columns(query=pl.lit("tail"))
                .rename({"h": "query_label", "tail_label": "answer_label"})
                .drop("tail_id")
            )
            df = (
                pl.concat([df_h, df_t])
                .group_by(["query_label", "query"], maintain_order=True)
                .agg(
                    [
                        "answer_label",
                    ]
                )
            )

            return df

    def get_answers(self) -> pl.DataFrame:
        """
        calculate a n-hot encoded list for the answers of a given query
        """
        df = self.df
        # expand dataframe and separate by query for head or query for tail
        df = df.explode(["answer_label"])

        # get answers dataframe depending on self.
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            # swap test and valid if models_swap is True
            if self.train_models_swap:
                test_answers = pl.read_csv(
                    self.valid_dir, separator="\t", new_columns=["h", "r", "t"]
                )
                valid_answers = pl.read_csv(
                    self.test_dir, separator="\t", new_columns=["h", "r", "t"]
                )
            else:
                test_answers = pl.read_csv(
                    self.test_dir, separator="\t", new_columns=["h", "r", "t"]
                )
                valid_answers = pl.read_csv(
                    self.valid_dir, separator="\t", new_columns=["h", "r", "t"]
                )
        else:
            test_answers = pl.read_csv(
                self.test_dir, separator="\t", new_columns=["h", "r", "t"]
            )

        # get answers for head and tail by merging with test_answers/valid_answers
        if self.query_answer == "head" or self.query_answer == "both":
            df_h = df.filter(pl.col("query") == "head")
            df_h = (
                df_h.join(
                    test_answers,
                    left_on=["query_label", "answer_label"],
                    right_on=["t", "h"],
                    how="left",
                )
                .with_columns(
                    pl.when(pl.col("r").is_null())
                    .then(0)
                    .otherwise(1)
                    .alias("in_testing")
                )
                .drop("r")
            )
            if self.valid_dir != None:
                # add validation column
                df_h = (
                    df_h.join(
                        valid_answers,
                        left_on=["query_label", "answer_label"],
                        right_on=["t", "h"],
                        how="left",
                    )
                    .with_columns(
                        pl.when(pl.col("r").is_null())
                        .then(0)
                        .otherwise(1)
                        .alias("in_validation")
                    )
                    .drop("r")
                )

        if self.query_answer == "tail" or self.query_answer == "both":
            df_t = df.filter(pl.col("query") == "tail")
            df_t = (
                df_t.join(
                    test_answers,
                    left_on=["query_label", "answer_label"],
                    right_on=["h", "t"],
                    how="left",
                )
                .with_columns(
                    pl.when(pl.col("r").is_null())
                    .then(0)
                    .otherwise(1)
                    .alias("in_testing")
                )
                .drop("r")
            )
            if self.valid_dir != None:
                # add validation column
                df_t = (
                    df_t.join(
                        valid_answers,
                        left_on=["query_label", "answer_label"],
                        right_on=["h", "t"],
                        how="left",
                    )
                    .with_columns(
                        pl.when(pl.col("r").is_null())
                        .then(0)
                        .otherwise(1)
                        .alias("in_validation")
                    )
                    .drop("r")
                )
        if self.query_answer == "head":
            df_h = df_h.group_by(["query_label", "query"], maintain_order=True).agg(
                ["answer_label", "in_testing", "in_validation"]
            )

            return df_h
        elif self.query_answer == "tail":
            df_t = df_t.group_by(["query_label", "query"], maintain_order=True).agg(
                ["answer_label", "in_testing", "in_validation"]
            )

            return df_t
        else:
            df = pl.concat([df_h, df_t])
            df = df.group_by(["query_label", "query"], maintain_order=True).agg(
                ["answer_label", "in_testing", "in_validation"]
            )

            return df

    @staticmethod
    def get_rank_from_position(x: List[int]) -> np.array:
        """
        Gets positions of all hits and returns as an array. Accounts for ties, rank starts at 1.

        Example
        ------------
        hits_list = [0,1,1,0,1,0,0,1]
        get_rank_from_position(hits_list) -> [2,2,3,5]
        """
        # add 1 to all ranks
        # subtract the number of true entities prior to the current entity
        x = np.array(list(np.where(x == 1)[0]))
        return (np.subtract(np.add(x, 1), np.array(list(np.where(x >= 0))[0]))).tolist()

    @staticmethod
    def get_true_index(x: List[int]) -> np.array:
        """
        Gets the index of all hits and returns as as an array.

        Example
        ------------
        hits_list = [0,1,1,0,1,0,0,1]
        get_true_index(hits_list) -> [1,2,4,7]
        """
        return list(np.where(x == 1)[0])

    def get_rank(
        self,
    ) -> pl.DataFrame:
        """
        Takes a dataframe of PyKEEN model predictions and returns dataframe with list of  rank vals
        """

        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"

        res_col = "in_testing" if self.group == "test" else "in_validation"
        drop_col = "in_validation" if self.group == "test" else "in_testing"
        if res_col not in self.df.columns:
            results_df = self.get_answers()
            df = results_df.unique(["query_label", "query"])
        else:
            df = self.df

        df = df.with_columns(
            pl.col(res_col)
            .map_elements(lambda x: self.get_rank_from_position(x))
            .alias("answer_filt_rank")
        ).drop(drop_col)

        return df

    def get_mrr(
        self,
    ) -> float:
        """
        Given a pykeen returned dataframe, get the MRR from known ranks
        """
        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"

        results_df = self.df

        if "answer_filt_rank" not in results_df.columns:
            results_df = self.get_rank()

        all_ranks = [
            float(j) for i in results_df["answer_filt_rank"].to_list() for j in i
        ]  # np.reciprocal requires floats, using ints leads to unexpected behavior
        all_rr = np.reciprocal(all_ranks)
        mrr = np.mean(all_rr)

        self.mrr = mrr

        return mrr

    def get_hits_at_k(self, k: int = 10) -> float:
        """
        Given a pykeen returned dataframe, get the hits at k from known trues
        * default k value is 10
        """
        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"

        results_df = self.df

        if "answer_filt_rank" not in results_df.columns:
            results_df = self.get_rank()

        all_ranks = [
            float(j) for i in results_df["answer_filt_rank"].to_list() for j in i
        ]
        k_array = np.full(np.array(all_ranks).shape, k, dtype=float)
        hitsk = np.mean(
            np.less_equal(
                all_ranks,
                k_array,
            )
        )

        return hitsk

    def extract_answers_from_rank(self) -> pl.DataFrame:
        """
        Extracts answers from rank dataframe
        """
        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"

        results_df = self.df

        group = "in_testing" if self.group == "test" else "in_validation"

        results_df = results_df.with_columns(
            pl.col(group)
            .map_elements(lambda x: self.get_true_index(x))
            .alias("known_trues")
        )

        # get dataframe of query and answer labels
        results_df = (
            results_df.explode(["answer_filt_rank", "known_trues"])
            .with_columns(
                pl.col("answer_label").list.get(pl.col("known_trues")).alias("answers")
            )
            .drop(group)
        )

        return results_df

    def extract_relative_year(self) -> pl.DataFrame:
        """
        Extracts relative year from indications
        """
        # get results from self.extract_answers_from_rank
        results_df = self.answer_df

        # get local directory to import indications
        ind_dir = pathlib.Path(self.dataset.training_path).parent.joinpath(
            "indications.parquet"
        )
        ind = pl.read_parquet(
            ind_dir,
            columns=["compound_semmed_id", "disease_semmed_id", "year_diff"],
        ).rename({"compound_semmed_id": "h", "disease_semmed_id": "t"})

        # merge indications with results_df
        if self.query_answer == "both" or self.query_answer == "head":  # (?, r, t)
            results_df_head = results_df.filter(pl.col("query") == "head")
            results_df_head = results_df_head.join(
                ind, left_on=["query_label", "answers"], right_on=["t", "h"], how="left"
            )

        if self.query_answer == "both" or self.query_answer == "tail":  # (h, r, ?)
            results_df_tail = results_df.filter(pl.col("query") == "tail")
            results_df_tail = results_df_tail.join(
                ind, left_on=["query_label", "answers"], right_on=["h", "t"], how="left"
            )

        if self.query_answer == "head":
            return results_df_head

        elif self.query_answer == "tail":
            return results_df_tail
        else:
            a_df = pl.concat([results_df_head, results_df_tail])
            return a_df
