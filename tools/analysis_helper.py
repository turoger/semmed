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

import numpy as np
import pandas as pd
import polars as pl
import pykeen
import pykeen.datasets.timeresolvedkg as trkg
import pykeen.models
import torch
from pykeen.predict import predict_all, predict_target

###
# Helper functions for analysing pykeen outputs
# for more information please see notebook: "03_generate_pykeen_output.ipynb"

### Available functions
# load_pykeen_train() -> initializes pykeen train set from a checkpoint
# load_pykeen_test() -> initializes pykeen test set from a checkpoint
# predict_on_test() -> generates tail predictions from loaded test h,r
# get_rank_from_position() -> returns rank of known answers as a list for each triple
# get_rank() -> returns rank of a series
# get_mrr() -> returns a float, mean reciprocal rank of the entire test set based on known answers
# get_hits_at_k() -> returns a float, reports the fraction of known answers less than or equal to k
#
###


class AnalysisHelper(object):
    """ """

    def __init__(
        self,
        build_dataset_kwargs: Optional[Mapping[str, Any]] = {},
        year: str = "1950",
        train_models_swap: bool = False,
    ):
        self.build_dataset_kwargs = build_dataset_kwargs
        self.year = year
        self.train_models_swap = train_models_swap
        self.dataset = self.load_pykeen_dataset()

        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            self.train, self.test, self.valid = self.load_pykeen_triples()
        else:
            self.train, self.test = self.load_pykeen_triples()

    def load_pykeen_dataset(self) -> "TimeResolvedKG":
        """
        Takes parameters used to build the dataset and a given year to import PykEEN dataset class
        """

        return trkg.TimeResolvedKG(
            build_dataset_kwargs=self.build_dataset_kwargs, year=self.year
        )

    def load_pykeen_triples(
        self,
    ) -> Tuple["TriplesFactory", "TriplesFactory", Optional["TriplesFactory"]]:
        """
        Takes parameters used to build the dataset and a given year to import PykEEN triplesfactory

        """
        the_dataset = self.load_pykeen_dataset()

        train = the_dataset.training
        test = the_dataset.testing

        # return train, test, valid if split_ttv is not 0
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            # swap test and valid if models_swap is True
            if self.train_models_swap:
                test = the_dataset.validation
                valid = the_dataset.testing
            else:
                test = the_dataset.testing
                valid = the_dataset.validation

            return train, test, valid

        else:
            return train, test

    def predict_qa(
        self,
        df: pd.DataFrame,
        # hr: Optional[Mapping[int, int] | Mapping[str, str]],
        # rt: Optional[Mapping[int, int] | Mapping[str, str]],
        answer: Optional[str] = "both",
    ):
        """
        Gets "h","t", or "both" predictions for a given dataframe of h,r,t triples
        returns a dataframe with the predictions
        """
        if answer != None:
            assert answer in [
                "head",
                "tail",
                "both",
            ], "answer must be one of ['head','tail','both']"

        answer_dict = {
            "head": dict(zip(df["t"], df["r"])),
            "tail": dict(zip(df["h"], df["r"])),
        }
        if answer == "head" or answer == "both":
            t_res_ls = list()
            for disease, relation in answer_dict["head"].items():
                pred = pykeen.predict.predict_target(
                    model=pykeen_model,
                    tail=disease,
                    relation=relation,
                    triples_factory=train,
                ).add_membership_columns(
                    testing=test,
                    validation=(
                        valid
                        if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                        else None
                    ),
                )  # check if tail predictions are in 'predict_triple' (correct answer)
                pred = pl.DataFrame(pred.df)
                pred = pred.with_columns(h=pl.lit(compound))
                h_res_ls.append(pred)
            df_t = pl.concat(h_res_ls)

        elif answer == "tail" or answer == "both":
            h_res_ls = list()
            for compound, relation in answer_dict["tail"].items():
                pred = pykeen.predict.predict_target(
                    model=pykeen_model,
                    head=compound,
                    relation=relation,
                    triples_factory=train,
                ).add_membership_columns(
                    testing=test,
                    validation=(
                        valid
                        if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                        else None
                    ),
                )  # check if tail predictions are in 'predict_triple' (correct answer)
                pred = pl.DataFrame(pred.df)
                pred = pred.with_columns(h=pl.lit(compound))
                t_res_ls.append(pred)
            df_h = pl.concat(t_res_ls)

    def predict_on(
        self,
        pykeen_model: pykeen.models.base.Model,
        group: Optional[str] = "test",  # {test,valid},
        query_answer: Optional[str] = "both",  # {both,tail,head}
    ) -> pl.DataFrame:
        """
        Takes a PyKEEN model and testing path to generate all compound indications on test set.
        group can be 'test' or 'valid' splits depending on the group to be assessed
        query_answer can be 'both', 'tail', 'head'. 'both' returns both head and tail predictions, 'tail' returns only tail predictions, 'head' returns only head predictions
        """
        self.group = group

        # setup train, test, valid if there is a validation set available
        if self.build_dataset_kwargs.get("split_ttv", 0) != 0:
            train, test, valid = self.load_pykeen_triples()
            t_pred_agg_ls = ["tail_label", "score", "in_testing", "in_validation"]
            h_pred_agg_ls = ["head_label", "score", "in_testing", "in_validation"]
        else:
            assert (
                group == "test"
            ), "No validation set found, in `self.build_dataset_kwargs` please use test group"
            train, test = self.load_pykeen_triples()
            t_pred_agg_ls = ["tail_label", "score", "in_testing"]
            h_pred_agg_ls = ["head_label", "score", "in_testing"]

        # import test or valid triples depending on 'group'
        predict_path = (
            self.dataset.testing_path
            if group == "test"
            else self.dataset.validation_path
        )

        # DataFrame for predictions
        predict_df = pd.read_csv(predict_path, sep="\t", names=["h", "r", "t"])

        # check if query_answer is valid
        if query_answer != None:
            assert query_answer in [
                "head",
                "tail",
                "both",
            ], "query_answer must be one of ['head','tail','both']"
            self.query_answer = query_answer
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
                ).add_membership_columns(
                    testing=test,
                    validation=(
                        valid
                        if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                        else None
                    ),
                )  # check if tail predictions are in 'predict_triple' (correct answer)
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
                ).add_membership_columns(
                    testing=test,
                    validation=(
                        valid
                        if self.build_dataset_kwargs.get("split_ttv", 0) != 0
                        else None
                    ),
                )  # check if tail predictions are in 'predict_triple' (correct answer)
                pred = pl.DataFrame(pred.df)
                pred = pred.with_columns(h=pl.lit(compound))
                t_res_ls.append(pred)
            df_t = pl.concat(t_res_ls)

        torch.cuda.empty_cache()
        if query_answer == "head":
            df_h = (
                df_h.with_columns(query=pl.lit("head"))
                .rename({"t": "query_label", "head_label": "answer_label"})
                .drop("head_id")
                .group_by(["query_label", "query"], maintain_order=True)
                .agg(["answer_label", "in_testing", "in_validation"])
            )

            self.df = df_h
            return df_h

        elif query_answer == "tail":
            df_t = (
                df_t.with_columns(query=pl.lit("tail"))
                .rename({"h": "query_label", "tail_label": "answer_label"})
                .drop("tail_id")
                .group_by(["query_label", "query"], maintain_order=True)
                .agg(["answer_label", "in_testing", "in_validation"])
            )

            self.df = df_t
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
                .agg(["answer_label", "in_testing", "in_validation"])
            )
            self.df = df
            return df

    @staticmethod
    def get_rank_from_position(x: List[int]) -> np.array:
        """
        Gets positions of all hits and returns as an array. Accounts for ties, rank starts at 1.

        Example
        ------------
        hits_list = [0,1,1,0,1,0,0,1]
        get_rank_from_position(hits_list) -> [2,2,4,7]
        """
        a = np.array(x)
        a = np.where(a == 1)
        a = a[0].tolist()
        b = [v - i + 1 for i, v in enumerate(a)]

        return b

    @staticmethod
    def get_true_index(x: List[int]) -> np.array:
        """
        Gets the position of all hits and returns as as an array.

        Example
        ------------
        hits_list = [0,1,1,0,1,0,0,1]
        get_true_index(hits_list) -> [1,2,4,7]
        """
        a = np.array(x)
        return np.where(a == 1)[0].tolist()

    def get_rank(
        self,
    ) -> pd.DataFrame:
        """
        Takes a dataframe of PyKEEN model predictions and returns dataframe with list of  rank vals
        """

        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"
        results_df = self.df
        df = results_df.unique(["query_label", "query"])

        res_col = "in_testing" if self.group == "test" else "in_validation"

        df = df.with_columns(
            pl.col(res_col)
            .map_elements(lambda x: self.get_rank_from_position(x))
            .alias("known_ranks")
        )
        self.df = df
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

        if "known_ranks" not in results_df.columns:
            results_df = self.get_rank()

        all_ranks = [
            float(j) for i in results_df["known_ranks"].to_list() for j in i
        ]  # np.reciprocal requires floats, using ints leads to unexpected behavior
        all_rr = np.reciprocal(all_ranks)
        mrr = np.mean(all_rr)

        self.mrr = mrr

        return mrr

    def get_hits_at_k(self, k: int = 10) -> float:
        """
        Given a pykeen returned dataframe, get the hits at k from known ranks
        * default k value is 10
        """
        assert (
            type(self.df) == pl.DataFrame
        ), "No dataframe found, please run self.predict_on() first"

        results_df = self.df

        if "known_ranks" not in results_df.columns:
            results_df = self.get_rank()

        all_ranks = [float(j) for i in results_df["known_ranks"].to_list() for j in i]
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
            results_df.explode(["known_ranks", "known_trues"])
            .with_columns(
                pl.col("answer_label").list.get(pl.col("known_trues")).alias("answers")
            )
            .drop(group)
        )
        self.answers_df = results_df
        return results_df

    def extract_relative_year(self) -> pl.DataFrame:
        """
        Extracts relative year from indications
        """
        # get results from self.extract_answers_from_rank
        assert (
            type(self.answers_df) == pl.DataFrame
        ), "No dataframe found, please run self.extract_answers_from_rank() first"

        results_df = self.answers_df

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
            return pl.concat([results_df_head, results_df_tail])
