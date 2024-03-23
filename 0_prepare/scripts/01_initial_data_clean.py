import argparse
import gc
import pickle
import sys

import mygene
import pandas as pd
import polars as pl

sys.path.append("../tools")
import load_umls


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Initial data cleaning of SemMedDB.
        Run this script to remove most errors prior to hetnet processing""",
        usage="01_initial_data_clean.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-n",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )

    return parser.parse_args(args)


def main(args):
    print("Running 01_initia_data_clean.py (approx. 10 minutes)")
    print("Importing raw SemMedDB")

    # lazy frame extraction from polars
    sem_df = (
        pl.scan_csv(
            source=f"../data/semmed{args.semmed_version}.csv",
            separator=",",
            truncate_ragged_lines=True,
            ignore_errors=True,
            new_columns=[
                "PREDICATION_ID",
                "SENTENCE_ID",
                "PMID",
                "PREDICATE",
                "SUBJECT_CUI",
                "SUBJECT_NAME",
                "SUBJECT_SEMTYPE",
                "SUBJECT_NOVELTY",
                "OBJECT_CUI",
                "OBJECT_NAME",
                "OBJECT_SEMTYPE",
                "OBJECT_NOVELTY",
                "column_14",
                "column_15",
            ],
            schema={
                "PREDICATION_ID": pl.Utf8,
                "SENTENCE_ID": pl.Utf8,
                "PMID": pl.Int64,
                "PREDICATE": pl.Utf8,
                "SUBJECT_CUI": pl.Utf8,
                "SUBJECT_NAME": pl.Utf8,
                "SUBJECT_SEMTYPE": pl.Utf8,
                "SUBJECT_NOVELTY": pl.Int64,
                "OBJECT_CUI": pl.Utf8,
                "OBJECT_NAME": pl.Utf8,
                "OBJECT_SEMTYPE": pl.Utf8,
                "OBJECT_NOVELTY": pl.Int64,
                "column_14": pl.Utf8,
                "column_15": pl.Utf8,
            },
        )
        .select(
            [
                "PREDICATION_ID",
                "SENTENCE_ID",
                "PMID",
                "PREDICATE",
                "SUBJECT_CUI",
                "SUBJECT_NAME",
                "SUBJECT_SEMTYPE",
                "SUBJECT_NOVELTY",
                "OBJECT_CUI",
                "OBJECT_NAME",
                "OBJECT_SEMTYPE",
                "OBJECT_NOVELTY",
            ]
        )
        .drop_nulls()
        .collect()
    )

    print(f"... Number of Rows in SemMedDB: {sem_df.shape[0]:,}")
    print(f"... Number of Cols in SemMedDB: {sem_df.shape[1]:,}")

    # Get all the pmids and save them to a file
    pmids = set(sem_df["PMID"].unique().to_list())
    out = []
    print(f"... All unique PMIDs: {len(pmids):,}")
    for pmid in pmids:
        try:
            # PMIDs should be convertable to int, if not, probably corrupted so don't add
            out.append(int(pmid))
        except:
            pass

    print(
        f"... Remaining PMIDs after removing malformed PMIDs (not convertable to integers): {len(out):,}"
    )
    print(f"... Write pmid_list_{args.semmed_version}.txt to ../data/")

    with open(f"../data/pmid_list_{args.semmed_version}.txt", "w") as out_file:
        for pmid in out:
            out_file.write(str(pmid) + "\n")

    print("Complete.")
    print("\n")
    #
    # Expand pipes in subjects and objects
    #
    print("Start initial data cleaning")
    print("... Expand synonyms demarcated by pipes")
    print(
        f'... {sem_df.filter(pl.col("SUBJECT_CUI").str.contains("|", literal=True)).shape[0]:,} Number of "Subject CUI" lines with pipe in subject'
    )
    print(
        f'... {sem_df.filter(pl.col("OBJECT_CUI").str.contains("|", literal=True)).shape[0]:,} Number of "OBJECT_CUI" lines with pipe in object'
    )
    print(
        "... Checking if SUBJECT and OBJECT CUI and NAME columns are the same length."
    )
    # Check if each split for SUBJECT_CUI and SUBJECT_NAME are the same length per line
    assert (
        sem_df.with_columns(
            pl.col("SUBJECT_CUI").str.split("|"), pl.col("SUBJECT_NAME").str.split("|")
        )
        .filter((pl.col("SUBJECT_CUI").list.len() != pl.col("SUBJECT_NAME").list.len()))
        .shape[0]
        == 0
    ), "There are some SUBJECT_CUI and SUBJECT_NAME are of different lengths"
    # Check if each split for OBJECT_CUI and OBJECT_NAME are _NOT_ the same length per line
    assert (
        sem_df.with_columns(
            pl.col("OBJECT_CUI").str.split("|"), pl.col("OBJECT_NAME").str.split("|")
        )
        .filter((pl.col("OBJECT_CUI").list.len() != pl.col("OBJECT_NAME").list.len()))
        .shape[0]
        != 0
    ), "OBJECT_CUI and OBJECT_NAME are _NOT_ different lengths"
    print("... Splitting SUBJECT and OBJECT CUI/NAME by pipes.")
    # Use polars to split SUBJECT/OBJECT and their CUI/NAME by pipes, and exclude malformed OBJECT lines
    sem_df = (
        sem_df.with_columns(
            pl.col("SUBJECT_CUI").str.split("|"),
            pl.col("SUBJECT_NAME").str.split("|"),
            pl.col("OBJECT_CUI").str.split("|"),
            pl.col("OBJECT_NAME").str.split("|"),
        )
        .filter(  # checks if object lengths are the same, keep only same lengths
            (pl.col("OBJECT_CUI").list.len() == pl.col("OBJECT_NAME").list.len())
        )
        .explode(["SUBJECT_CUI", "SUBJECT_NAME"])
        .explode(["OBJECT_CUI", "OBJECT_NAME"])
        .unique()
    )
    # clear memory
    del pmids
    del out
    gc.collect()
    print(
        f"... Number of Rows in SemMedDB after initial data cleaning: {sem_df.shape[0]:,}"
    )
    print("Complete.")
    print("\n")

    # Normalize IDS for Genes to CUIs
    # Sometimes genes appear with a CUI as an identifier, other times they have an entrez gene id.
    # Use mygene.info has umls data as a reliable, up-to-date soruce for mapping. For those that cannot be acquired by mygene.info, we will use HGNC Mappings as UMLS contains those values
    print("Use MyGene.info to merge synonyms.")
    print("... Retrieving MyGene.info")
    print("... Getting Entrez Gene IDs")
    mg = mygene.MyGeneInfo()
    # Get all cuis (subjects | objects) that don't start with C
    genes_need_fixing = set(
        sem_df.filter(~pl.col("SUBJECT_CUI").str.starts_with("C"))["SUBJECT_CUI"]
    ).union(
        set(sem_df.filter(~pl.col("OBJECT_CUI").str.starts_with("C"))["OBJECT_CUI"])
    )
    print(f"... Number of genes that need fixing: {len(genes_need_fixing):,}")
    print("... Querying mygene.info for Entrez Gene IDs")
    mg_result = mg.getgenes(
        list(genes_need_fixing), fields="symbol,name,umls.cui,HGNC", dotfield=True
    )
    print("... Building Entrez to CUI map")
    mg_result = (
        pd.DataFrame(mg_result)
        .query("notfound != True")[["_id", "HGNC", "name", "symbol", "umls.cui"]]
        .explode("umls.cui")
        .rename(columns={"_id": "CUI", "umls.cui": "umls_cui"})
    )
    mg_result2 = pl.DataFrame(mg_result)

    e_to_cui = dict(
        zip(
            mg_result2.drop_nulls("umls_cui")["CUI"],
            mg_result2.drop_nulls("umls_cui")["umls_cui"],
        )
    )
    print(
        f"... {len(e_to_cui):,} out of {len(genes_need_fixing):,} Entrez IDs can be mapped to CUI via mygene.info"
    )
    print("Complete. \n")
    del mg
    gc.collect()

    # Get some more mappings from umls
    # Although UMLS does not have direct Entrez Gene IDs mappings to UMLS CUIs, it does have HGNC IDs.
    # Some Entrez to HGNC values were picked up from mygene, so they will be used to further increase the maps size.
    print("Use HGNC to merge synonyms.")
    print("... Retrieving HGNC")
    print("... creating a CUI to HGNC map")
    # Create a CUI to HGNC map
    need_map = genes_need_fixing - set(e_to_cui.keys())
    e_to_hgnc = dict(
        zip(
            (
                x := mg_result2.drop_nulls("HGNC")
                .with_columns(("HGNC:" + pl.col("HGNC")).alias("HGNC"))
                .filter(pl.col("CUI").is_in(need_map))
            )["CUI"],
            x["HGNC"],
        )
    )
    hgnc_ids = list(e_to_hgnc.values())
    print(
        f"... {len(hgnc_ids):,} out of {len(need_map):,} Entrez IDs can be mapped to HGNC via mygene.info"
    )
    print("... mapping CUI to HGNC from the UMLS metathesaurus")
    conso = load_umls.open_mrconso()
    q_res = conso.filter(pl.col("SCUI").is_in(hgnc_ids), pl.col("TTY") == "MTH_ACR")
    # q_res = conso.query('SCUI in @hgnc_ids and TTY == "MTH_ACR"')
    hgnc_to_cui = dict(zip(q_res["SCUI"], q_res["CUI"]))
    e_to_cui_1 = {
        k: hgnc_to_cui[v] for k, v in e_to_hgnc.items() if v in hgnc_to_cui.keys()
    }

    e_to_cui = {**e_to_cui_1, **e_to_cui}

    print(f"... {len(e_to_cui)} Entrez IDs now mapped")
    print("Complete. \n")

    print('Generate "cui_to_names" and "entrez_to_cui" files')
    print("... getting names for CUIs from SemMed file")
    # First get the names from semmed for everything that already has a CUI
    d = dict(
        zip(
            (x := sem_df.filter(pl.col("SUBJECT_CUI").str.starts_with("C")))[
                "SUBJECT_CUI"
            ].to_list(),
            x["SUBJECT_NAME"].to_list(),
        )
    )
    d2 = dict(
        zip(
            (x := sem_df.filter(pl.col("OBJECT_CUI").str.starts_with("C")))[
                "OBJECT_CUI"
            ].to_list(),
            x["OBJECT_NAME"].to_list(),
        )
    )
    c_to_name_dict = {**d, **d2}
    print(f"... {len(c_to_name_dict):,} CUIs have names in SemMedDB")

    # generate a list of cui values that isn't a list of list otherwise it cannot be hashed
    e_to_cui_vals = []
    for i in list(e_to_cui.values()):
        if type(i) == str:
            e_to_cui_vals.append(i)
        else:
            for j in i:
                e_to_cui_vals.append(j)
    # list comprehension because some values are lists which cannot be hashed
    need_name = set(e_to_cui_vals) - set(c_to_name_dict.keys())
    print(f"... {len(need_name):,} CUIs need names")
    # Get as many names as possible directly from UMLS
    # ISPREF == Y gets preferred names for preferred name
    c_to_name_1 = dict(
        zip(
            (
                x := conso.filter(
                    pl.col("CUI").is_in(need_name), pl.col("ISPREF") == "Y"
                )
            )["CUI"],
            x["STR"],
        )
    )
    print(f"... {len(c_to_name_1):,} CUIs have names in UMLS")
    # Add the two dictionaries together. The missing values can be used to query
    c_to_name_dict = {**c_to_name_1, **c_to_name_dict}
    to_query = set(e_to_cui_vals) - set(c_to_name_dict.keys())
    print(f"... {len(to_query):,} CUIs need names to be queried")

    # Most names are the Gene symbol + 'gene' so we'll use that for the remainder
    name_from_mygene = dict(
        zip(
            (x := mg_result2.filter(pl.col("umls_cui").is_in(to_query)))["umls_cui"],
            x["symbol"],
        )
    )  # returns a dict{umls_cui:symbol}
    # Make sure those mapped from mygene via HGNC have names
    to_query_1 = [k for k, v in hgnc_to_cui.items() if v in to_query]
    print(f"... number of missing cui-names to query in HGNC: {len(to_query_1)}")
    print(f"... querying HGNC for missing cui-names and update the dictionary")
    name_from_mygene = {k: v + " gene" for k, v in name_from_mygene.items()}
    # returns a dict{umls_cui:gene symbol}
    # Ensure that all mappable genes now have a mappable name
    c_to_name_dict = {**name_from_mygene, **c_to_name_dict}
    to_query = set(e_to_cui_vals) - set(c_to_name_dict.keys())
    assert len(to_query) == 0, "Not all genes have a name"
    # Check that the mapper produces the correct name when given the CUI for POMC gene
    assert (
        c_to_name_dict[e_to_cui["5443"]] == "POMC gene"
    ), "POMC gene is not mapped properly to '5443'"

    print('... Exporting "cui_to_name" and "entrez_to_cui" to ../data')
    pickle.dump(c_to_name_dict, open("../data/cui_to_name.pkl", "wb"))
    pickle.dump(e_to_cui, open("../data/entrez_to_cui.pkl", "wb"))
    print("Complete. \n")
    # clear variables to make space in memory
    del mg_result
    del mg_result2
    del to_query_1
    del name_from_mygene
    del hgnc_to_cui
    gc.collect()

    print("Applying Fixes to SemMed DataFrame")
    # apply the changes
    print("... before Entrez to CUI mapping")
    print(
        f'... number of non-CUI subject rows: {sem_df.filter(~pl.col("SUBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )
    print(
        f'.... number of non-CUI object rows: {sem_df.filter(~pl.col("OBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )
    print(
        f'... number of non-CUI subject or object rows: {sem_df.filter(~pl.col("SUBJECT_CUI").str.starts_with("C")|~pl.col("OBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )
    print("... mapping Entrez to CUI")
    # converts subjects/object from entrez id to cui_ids via e_to_cui dictionary if and only if the cui is not an entrez id
    sem_df2 = pl.concat(
        [
            sem_df.filter(
                pl.col("SUBJECT_CUI").str.starts_with("C"),
                pl.col("OBJECT_CUI").str.starts_with("C"),
            ),
            sem_df.filter(
                ~pl.col("SUBJECT_CUI").str.starts_with("C")
                | ~pl.col("OBJECT_CUI").str.starts_with("C")
            ).with_columns(
                pl.col("SUBJECT_CUI").replace(e_to_cui),
                pl.col("OBJECT_CUI").replace(e_to_cui),
                pl.col("SUBJECT_CUI").replace(c_to_name_dict).alias("SUBJECT_NAME"),
                pl.col("OBJECT_CUI").replace(c_to_name_dict).alias("OBJECT_NAME"),
            ),
        ]
    )
    print("... after Entrez to CUI Mapping")
    print(
        f'... number of non-CUI subject rows: {sem_df2.filter(~pl.col("SUBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )
    print(
        f'... number of non-CUI object rows: {sem_df2.filter(~pl.col("OBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )
    print(
        f'... number of non-CUI subject or object rows: {sem_df2.filter(~pl.col("SUBJECT_CUI").str.starts_with("C")|~pl.col("OBJECT_CUI").str.starts_with("C")).shape[0]:,}'
    )

    print(
        f"... seperately export unmapped CUIs from initial cleaned SemMedDB copy to ../data/semmed{args.semmed_version}_no_CUI.parquet"
    )
    sem_df2.filter(
        ~pl.col("SUBJECT_CUI").str.starts_with("C")
        | ~pl.col("OBJECT_CUI").str.starts_with("C")
    ).write_parquet(f"../data/semmed{args.semmed_version}_no_CUI.parquet")
    print(
        f"... seperately export cleaned SemMedDB copy to ../data/semmed{args.semmed_version}_clean.parquet"
    )
    sem_df = sem_df2.filter(
        pl.col("SUBJECT_CUI").str.starts_with("C"),
        pl.col("OBJECT_CUI").str.starts_with("C"),
    )
    sem_df.write_parquet(f"../data/semmed{args.semmed_version}_clean.parquet")
    print("Complete. \n")

    print("Remove Deprecated CUIs")
    # Remove Depricated CUIs
    # Get the map from old CUIs to new CUIs
    print("... getting retired CUIs")
    retired_cui = load_umls.open_mrcui()

    # Get the map from old CUIs to new CUIs conditioned on only occuring once (direct maps, no many to one or one to many)
    retired_cui = (
        retired_cui.sort("VER", descending=True)
        .group_by("CUI1", maintain_order=True)
        .agg(["VER", "REL", "CUI2"])
        .with_columns(pl.col("CUI2").list.len().alias("CUI2_len"))
        .filter(pl.col("CUI2_len") == 1)
        .with_columns(pl.col("CUI2").list.first())
    )
    print(f"... generate a old cui to new cui map")
    # Make a mapper from the old to the new
    cui_map = dict(zip(retired_cui["CUI1"].to_list(), retired_cui["CUI2"].to_list()))
    # ensure we have names for all new cuis
    no_name = set(cui_map.values()) - set(c_to_name_dict.keys())
    # Need to filter conso because some strings are not in UTF8 format
    filt_conso = (
        conso.filter(
            pl.col("LAT") == "ENG",
            pl.col("ISPREF") == "Y",
            pl.col("CUI").is_in(no_name),
        )
        .group_by("CUI", maintain_order=True)
        .agg(["STR"])
        .with_columns(pl.col("STR").list.first())
    )
    print(f"... number of New CUI mappings without a name: {len(no_name):,}")
    # ensure we have names for all new cuis
    if len(no_name) > 0:
        query_result = dict(
            zip(
                filt_conso["CUI"].to_list(),
                filt_conso["STR"].to_list(),
            )
        )

        c_to_name_dict.update(query_result)
        pickle.dump(c_to_name_dict, open("../data/cui_to_name.pkl", "wb"))

    print(
        "... {} concepts identifiers could not be mapped to a name".format(
            len(no_name) - len(query_result)
        )
    )

    # How many unique s-p-o triples before de-depreication?
    print(
        f"... {len(sem_df.unique(subset=['SUBJECT_CUI', 'PREDICATE', 'OBJECT_CUI'])):,} Unique S-P-O triples before de-deprecation"
    )
    print("... De-deprecating CUIs")
    # Map the depricated values to their new CUIs
    sem_df = sem_df.with_columns(
        pl.col("SUBJECT_CUI").replace(
            cui_map
        ),  # Map the depricated values to their new CUIs
        pl.col("OBJECT_CUI").replace(cui_map),
        pl.col("SUBJECT_NAME").replace(
            c_to_name_dict
        ),  # Ensure the names are now corrected
        pl.col("OBJECT_NAME").replace(c_to_name_dict),
    ).drop_nulls(
        ["SUBJECT_CUI", "OBJECT_CUI"]
    )  # Any removed CUIs should be taken out

    # How many unique spo triples after the corrections?
    print(
        f"... {len(sem_df.unique(subset=['SUBJECT_CUI', 'PREDICATE', 'OBJECT_CUI'])):,} Unique S-P-O triples after de-deprecation"
    )
    print(f"... Exporting SemMedDB, de-deprecated")
    sem_df.write_parquet(
        f"../data/semmed{args.semmed_version}_clean_de-deprecate.parquet"
    )
    print("Complete. 01_initial_data_clean.py has finished running.")


if __name__ == "__main__":
    main(parse_args())
