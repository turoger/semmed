import argparse
import pickle

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
        description=r"""Initial data cleaning of SemMedDB.
        Run this script to remove most errors prior to hetnet processing""",
        usage="03_umls_cui_to_mesh_descriptorID.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-n",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
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

    logger.info("Running 03_umls_cui_to_mesh_descriptorID.py")
    logger.info("... Import MRCONSO.RRF")
    df = pl.read_csv(
        source=f"../data/{args.umls_date}-full/{args.umls_date}/META/MRCONSO.RRF",
        separator="|",
        truncate_ragged_lines=True,
        ignore_errors=True,
        schema={
            "CUI": pl.Utf8,
            "LAT": pl.Utf8,
            "TS": pl.Utf8,
            "LUI": pl.Utf8,
            "STT": pl.Utf8,
            "SUI": pl.Utf8,
            "ISPREF": pl.Utf8,
            "AUI": pl.Utf8,
            "SAUI": pl.Utf8,
            "SCUI": pl.Utf8,
            "SDUI": pl.Utf8,
            "SAB": pl.Utf8,
            "TTY": pl.Utf8,
            "CODE": pl.Utf8,
            "STR": pl.Utf8,
            "SRL": pl.Utf8,
            "SUPPRESS": pl.Utf8,
            "CVF": pl.Utf8,
        },
    ).filter(pl.col("LAT") == "ENG", pl.col("ISPREF") == "Y", pl.col("SAB") == "MSH")
    logger.info("... Fix columns and extract CUI to MSH mappings")
    # create a cui to mesh mapping
    grpd = df.group_by("CUI").agg("SDUI").with_columns(pl.col("SDUI").list.unique())
    cui_to_msh = dict(zip(grpd["CUI"].to_list(), grpd["SDUI"].to_list()))
    logger.info(f"... Number of CUI to MeSH maps: {len(cui_to_msh):,}")
    logger.info("... Exporting UMLS CUI to MeSH mappings")
    # dump conversion
    with open("../data/UMLS-CUI_to_MeSH-Descripctor.pkl", "wb") as f:
        pickle.dump(cui_to_msh, f)

    # extract out the cui to more than 1 msh ID
    one_2_one_grpd = grpd.filter(pl.col("SDUI").list.len() == 1).with_columns(
        pl.col("SDUI").list.first()
    )

    cui_to_msh_1t1 = dict(
        zip(one_2_one_grpd["CUI"].to_list(), one_2_one_grpd["SDUI"].to_list())
    )

    cuis = list(cui_to_msh_1t1.keys())
    to_name = df.filter(pl.col("CUI").is_in(cuis))

    msh_to_name = dict(zip((x := to_name.unique("SDUI"))["SDUI"], x["STR"]))
    logger.info(
        "... Exporting quick and dirty name map from CUI to MeSH for CUI to multiple MeSH IDs"
    )
    with open("../data/MeSH_to_name_quick_n_dirty.pkl", "wb") as f:
        pickle.dump(msh_to_name, f)

    logger.info("Complete. 03_umls_cui_to_mesh_descriptorID.py has finished running.\n")


if __name__ == "__main__":
    main(parse_args())
