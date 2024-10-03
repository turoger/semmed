import argparse
import pickle
import sys

import polars as pl

sys.path.append("../tools/")
import load_umls

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
        description=r"""Initial data cleaning of SemMedDB.
        Run this script to remove most errors prior to hetnet processing""",
        usage="05_mesh_id_to_name_via_umls.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-d",
        "--umls_date",
        default="2023AA",
        type=str,
        help="downloaded semmed version year followed by two capitalized, alphabetical characters",
    )

    return parser.parse_args(args)


def main(args):
    logger.info("Running 05_mesh_id_to_name_via_umls.py")
    logger.info("... Loading umls files")
    conso = load_umls.open_mrconso(
        f"../data/{args.umls_date}-full/{args.umls_date}/META/"
    )
    msh_rows = conso.filter(pl.col("SAB") == "MSH")

    logger.info("... Loading old MeSH mappings to reprocess")
    with open("../data/MeSH_to_name_quick_n_dirty.pkl", "rb") as f:
        msh_to_name_old = pickle.load(f)

    logger.info("... updating MeSH id to names")
    # Least to most important / redundant
    msh_to_name = {}

    # Least to most important / redundant
    for tty in ["PXQ", "PEP", "PCE", "HT", "QEV", "NM", "MH"]:
        q_res = msh_rows.filter(pl.col("LAT") == "ENG", pl.col("TTY") == tty)
        msh_to_name.update(dict(zip(q_res["SDUI"].to_list(), q_res["STR"].to_list())))

    msh_to_name_final = {**msh_to_name_old, **msh_to_name}
    assert len(msh_to_name) == msh_rows["SDUI"].n_unique()
    logger.info(
        f"... Quick n Dirty MeSH concepts mapped to names: {len(msh_to_name_old):,}"
    )
    logger.info(f"... MeSH concepts mapped to names now: {len(msh_to_name):,}")
    logger.info(
        f"... Total MeSH concepts with mapped names: {len(msh_to_name_final):,}"
    )
    pickle.dump(msh_to_name_final, open("../data/MeSH_id_to_name_via_UMLS.pkl", "wb"))
    logger.info("Complete. 05_Mesh_id_to_name_via_umls.py has finished running. \n")


if __name__ == "__main__":
    main(parse_args())
