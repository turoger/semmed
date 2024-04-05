import numpy as np
import pandas as pd
import polars as pl
import pykeen
import torch
from pykeen.constants import PYKEEN_CHECKPOINTS
from pykeen.pipeline import pipeline
from pykeen.predict import predict_all

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


def load_pykeen_train(chkpt: str, train_path: str):
    """
    Takes a PyKEEN checkpoint and training path to import a PyKEEN model
    """
    chkpt = torch.load(PYKEEN_CHECKPOINTS.joinpath(chkpt))
    train = pykeen.triples.TriplesFactory.from_path(
        path=train_path,
        entity_to_id=chkpt["entity_to_id_dict"],
        relation_to_id=chkpt["relation_to_id_dict"],
        create_inverse_triples=True,
    )
    return train


def load_pykeen_test(chkpt: str, test_path: str):
    """
    Takes a PyKEEN checkpoint and training path to import a PyKEEN model
    """
    chkpt = torch.load(PYKEEN_CHECKPOINTS.joinpath(chkpt))
    test = pykeen.triples.TriplesFactory.from_path(
        path=test_path,
        entity_to_id=chkpt["entity_to_id_dict"],
        relation_to_id=chkpt["relation_to_id_dict"],
        create_inverse_triples=True,
    )
    return test


def predict_on_test(
    pykeen_model: pykeen.models.base.Model,
    train: "TriplesFactory",
    test: "TriplesFactory",
    test_path: str,
) -> pd.DataFrame:
    """
    Takes a PyKEEN model and testing path to generate all compound indications on test set
    """
    test_df = pd.read_csv(test_path, sep="\t", names=["h", "r", "t"])
    test_dict = dict(zip(test_df["h"], test_df["r"]))

    res_ls = list()
    for compound, relation in test_dict.items():
        pred = pykeen.predict.predict_target(
            model=pykeen_model,
            head=compound,
            relation=relation,
            triples_factory=train,
        ).add_membership_columns(testing=test)
        pred = pred.df
        pred["h"] = compound
        res_ls.append(pred.groupby("h").agg(list).reset_index())
        # res_ls.append(pred)
    df = pd.concat(res_ls)
    torch.cuda.empty_cache()
    return df


def get_rank_from_position(x: list) -> np.array:
    """
    Gets positions of all hits and returns as an array

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


def get_rank(results_df: pd.DataFrame, names: bool = False) -> pd.DataFrame:
    """
    Takes a dataframe of PyKEEN model predictions and returns dataframe with list of  rank vals
    names: {True|False} returns results as dictionary {'in_testing_item':'rank'}
    """
    df = results_df.drop_duplicates(subset=["h"])
    known_ranks = df.in_testing.apply(lambda x: get_rank_from_position(x))

    if names == True:
        target_indices = [
            [list(df.tail_label)[i][ii] for ii, b in enumerate(ind) if b == True]
            for i, ind in enumerate(list(df.in_testing))
        ]

        df["known_ranks"] = [
            dict(zip(val, list(known_ranks)[ind]))
            for ind, val in enumerate(target_indices)
        ]

    else:
        df["known_ranks"] = known_ranks

    return df


def get_mrr(results_df: pd.DataFrame) -> float:
    """
    Given a pykeen returned dataframe, get the MRR from known ranks
    """
    if "known_ranks" not in results_df.columns:
        results_df = get_rr(results_df)

    all_ranks = [
        float(j) for i in results_df.known_ranks for j in i
    ]  # np.reciprocal requires floats, using ints leads to unexpected behavior
    all_rr = np.reciprocal(all_ranks)
    mrr = np.mean(all_rr)

    return mrr


def get_hits_at_k(results_df: pd.DataFrame, k: int = 10) -> float:
    """
    Given a pykeen returned dataframe, get the hits at k from known ranks
    * default k value is 10
    """
    if "known_ranks" not in results_df.columns:
        results_df = get_rr(results_df)

    all_ranks = [float(j) for i in results_df.known_ranks for j in i]
    k_array = np.full(np.array(all_ranks).shape, k, dtype=float)
    hitsk = np.mean(
        np.less_equal(
            all_ranks,
            k_array,
        )
    )

    return hitsk
