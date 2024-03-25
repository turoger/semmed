# Code adapting 01-building_the_hetnet jupyter notebook.
# Meant as a script to download everything without going through the notebook
# This script will generate a preliminarily cleaned SemMed Dataset node and edges file
import argparse
import os
import sys

import polars as pl

sys.path.append("../tools")
import load_umls


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Build SemMed Heterogenous Graph
        Use this script to preliminarily process, clean and build the SemMed textmined knowledge graph for time-based knowledge graph link prediction. This script will output a node and edges file of the graph with the following headers:
        * nodes:
        ['id','name','label'] -> where id, name and label corresponds to the unique identifier, object name, and object type
        * edges:
        ['start_id','end_id','type'] -> where start_id, end_id, and type corresponds to the start node unique id, end node unique id, and relation
        """,
        usage="01_build_hetnet_polars.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-v",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help='SemMed dump version number to process, a string ie."VER43_R"',
    )
    parser.add_argument(
        "-n",
        "--drop_negative_relations",
        default=False,
        action="store_true",
        help='Remove negative relations from output. If "convert_negative_relations" is True, negative relations will be converted to bidirectional',
    )
    parser.add_argument(
        "-c",
        "--convert_negative_relations",
        default=False,
        action="store_true",
        help='Converts negative relations to bidirectional relations. i.e. "NEG_ISA" -> "ISA"',
    )
    parser.add_argument(
        "-d",
        "--include_direction",
        default=False,
        action="store_true",
        help='Includes or removes direction component of metapath. i.e "CDpo>G" -> "CDpoG", "Cct>C" -> "CctC". Directionality cannot be applied if "convert_negative_relations" is False or "drop_negative_relations" is False',
    )
    return parser.parse_args(args)


def main(args):
    print(f"Running 01_build_hetnet_polars.py")
    print(f"--semmed_version: {args.semmed_version}")
    print(f"--drop_negative_relations: {args.drop_negative_relations}")
    print(f"--convert_negative_relations: {args.convert_negative_relations}")
    print(f"--include_direction: {args.include_direction}\n")
    print(f"... Reading in SemMed Dataframe.")
    # read dataframe
    sem_df = (
        pl.read_parquet(
            os.path.join(
                "../data/", f"semmed{args.semmed_version}_clean_de-deprecate.parquet"
            ),
        )
        .unique(  # Remove duplicated objects
            subset=[
                "SUBJECT_CUI",
                "SUBJECT_NAME",
                "SUBJECT_SEMTYPE",
                "PREDICATE",
                "OBJECT_CUI",
                "OBJECT_NAME",
                "OBJECT_SEMTYPE",
            ]
        )
        .filter(  # Removing generalized concepts
            pl.col("SUBJECT_NOVELTY") != 0, pl.col("OBJECT_NOVELTY") != 0
        )
    )
    # some stats
    print(f"... Rows: {sem_df.shape[0]:,}")
    print(f"... Cols: {sem_df.shape[1]}")
    print(f"... Column names: {sem_df.columns}")

    # generate pmid df from edges
    print("... Generating triple to PMID list")
    pmids = (
        sem_df.group_by(["SUBJECT_CUI", "PREDICATE", "OBJECT_CUI"])
        .agg(["PMID"])
        .with_columns(pl.col("PMID").list.unique())
        .with_columns(pl.col("PMID").list.len().alias("len_PMID"))
    )
    print(f"Complete. \n")
    print(f"Process node in SemMed")
    # fix issues with multiple semtypes per cui
    # umls abbreviations to semtype
    print("... Creating mappings between UMLS abbreviations and semantic types")
    abbv_to_type = dict()
    with open("../data/SemanticTypes_2018AB.txt") as fin:
        for line in fin:
            line = line.strip()
            lspt = line.split("|")
            abbv_to_type[lspt[0]] = lspt[-1]
    type_to_abbv = {v: k for k, v in abbv_to_type.items()}

    # umls semtype to supertype mapping
    print("... Creating mappings between UMLS semantic types and super types")
    abbv_to_super = dict()
    with open("../data/SemGroups_2018.txt") as fin:
        for line in fin:
            line = line.strip()
            lspt = line.split("|")
            abbv_to_super[type_to_abbv[lspt[-1]]] = lspt[1]

    # fix mapping issues with unused abbreviations. HARD CODED. See notebook in how these were obtained.
    # cells 15-26
    # Looked ups a few cuis with these and they are all of the living beings type,
    # so we will set these to that semtype
    print("... fixing UMLS abbreviation mappings (hard coded)")
    abbv_to_super["alga"] = "Living Beings"
    abbv_to_super["invt"] = "Living Beings"
    abbv_to_super["rich"] = "Living Beings"
    # a few of them are chemicals & Drugs
    abbv_to_super["carb"] = "Chemicals & Drugs"
    abbv_to_super["eico"] = "Chemicals & Drugs"
    abbv_to_super["lipd"] = "Chemicals & Drugs"
    abbv_to_super["nsba"] = "Chemicals & Drugs"
    abbv_to_super["opco"] = "Chemicals & Drugs"
    abbv_to_super["strd"] = "Chemicals & Drugs"
    # these are Genes & Molecular Sequences
    abbv_to_super["C0030193"] = "Genes & Molecular Sequences"

    #
    print("... Generating nodes file from edges")
    snodes = sem_df[["SUBJECT_CUI", "SUBJECT_NAME", "SUBJECT_SEMTYPE"]].rename(
        {"SUBJECT_CUI": "id", "SUBJECT_NAME": "name", "SUBJECT_SEMTYPE": "label"}
    )
    onodes = sem_df[["OBJECT_CUI", "OBJECT_NAME", "OBJECT_SEMTYPE"]].rename(
        {"OBJECT_CUI": "id", "OBJECT_NAME": "name", "OBJECT_SEMTYPE": "label"}
    )
    nodes = (
        snodes.vstack(onodes)
        .unique()
        .with_columns(pl.col("label").replace(abbv_to_super))
    )
    print(f"... Unique node types: {nodes['label'].unique().to_list()}")
    print(
        f"... There are {nodes.shape[0]:,} nodes and {nodes['id'].unique().shape[0]:,} unique IDs"
    )
    print(
        f"... {nodes.group_by('id').len().filter(pl.col('len')>1).shape[0]:,} IDs have been found to have multiple semantic types"
    )

    # fix semantic types using the UMLS Metathesaurus
    print("... fix semantic types using UMLS")
    print("... Loading MRSTY to make a CUI to TUI map")
    mrsty = load_umls.open_mrsty()
    cui_to_tui_df = (  # unique cui to [tui,tui] dataframe
        mrsty.unique()
        .group_by("CUI")
        .agg("TUI")
        .with_columns(pl.col("TUI").list.unique())
        .with_columns(pl.col("TUI").list.len().alias("len_tui"))
    )
    print(f"... Loaded {cui_to_tui_df.shape[0]:,} CUI to TUI mappings")
    print(
        f"... Number of CUIs with more than one TUI: {cui_to_tui_df.filter(pl.col('len_tui')>1).shape[0]:,}"
    )

    print("... Getting TUI to super semantic type map")
    t_code_to_super = dict()
    with open("../data/SemGroups_2018.txt") as fin:
        for line in fin:
            line = line.strip()
            ls = line.split("|")
            t_code_to_super[ls[2]] = ls[1]

    cui_to_tui_df = (
        cui_to_tui_df.explode("TUI")
        .with_columns(
            pl.col("TUI")
            .replace(t_code_to_super)
            .alias("super")  # map TUI to super and name column as 'super'
        )
        .group_by("CUI")
        .agg(["TUI", "super"])
        .with_columns(  # get list of TUI and super for each CUI
            pl.col("TUI").list.unique(), pl.col("super").list.unique()
        )
        .with_columns(  # get length of each list
            pl.col("TUI").list.len().alias("len_tui"),
            pl.col("super").list.len().alias("len_super"),
        )
    )
    issues = cui_to_tui_df.filter(pl.col("len_super") > 1)
    print(f"... There are {issues.shape[0]:,} CUIs with multiple Supertype")

    print("... Fixing multiple super semantic type maps")
    issues = issues.with_columns(pl.col("super").list.sort())
    issue_types = [
        eval(i) for i in sorted(list(set([str(v) for v in issues["super"].to_list()])))
    ]
    supclass_to_df = {
        str(i): issues.filter(pl.col("super") == pl.Series([i])) for i in issue_types
    }
    # This is mainly HARD CODED
    fixed_sem = {}
    fixed_sem["C0025131"] = "Occupations"
    fixed_sem["C0152027"] = "Disorders"
    fixed_sem["C0524486"] = "Living Beings"
    # update the dictionary
    fixed_sem.update(
        dict(
            zip(
                supclass_to_df["['Chemicals & Drugs', 'Living Beings']"]
                .join(nodes, left_on="CUI", right_on="id", how="left")
                .filter(pl.col("name").is_not_null())["CUI"]
                .to_list(),
                ["Chemicals & Drugs"] * 2,
            )
        )
    )
    fixed_sem.update(
        dict(
            zip(
                ["C1328049", "C0879593", "C5417935", "C1708270", "C1516687"],
                ["Chemicals & Drugs"] * 4 + ["Anatomy"],
            )
        )
    )
    fixed_sem.update(
        dict(
            zip(
                cui := supclass_to_df["['Chemicals & Drugs', 'Objects']"]
                .join(nodes[["id", "name"]], left_on="CUI", right_on="id", how="left")
                .unique("CUI")
                .filter(pl.col("name").is_not_null())["CUI"]
                .to_list(),
                ["Objects"] * len(cui),
            )
        )
    )
    # All these items are concepts, and therefore objects. update accordingly
    fixed_sem.update(
        dict(
            zip(
                cui := supclass_to_df["['Concepts & Ideas', 'Objects']"]
                .join(nodes[["id", "name"]], left_on="CUI", right_on="id", how="left")
                .unique("CUI")
                .filter(pl.col("name").is_not_null())["CUI"]
                .to_list(),
                ["Concepts & Ideas"] * len(cui),
            )
        )
    )
    # All these items are organizations, and therefore objects. update accordingly
    fixed_sem.update(
        dict(
            zip(
                cui := supclass_to_df["['Objects', 'Organizations']"]
                .join(nodes[["id", "name"]], left_on="CUI", right_on="id", how="left")
                .unique("CUI")
                .filter(pl.col("name").is_not_null())["CUI"]
                .to_list(),
                ["Organizations"] * len(cui),
            )
        )
    )
    print(f"... Total fixed semanic types: {len(fixed_sem):,}")
    print(f"... Generating dictionary mappings to relabel semantic types")
    cui_to_tui_df = cui_to_tui_df[["CUI", "super"]].explode("super")
    cui_to_tui_dict = dict(
        zip(cui_to_tui_df["CUI"].to_list(), cui_to_tui_df["super"].to_list())
    )
    cui_to_tui_dict.update(fixed_sem)
    before_len = len(nodes)
    nodes_dict = dict(zip(nodes["id"].to_list(), nodes["label"].to_list()))
    nodes_dict.update(cui_to_tui_dict)
    print("... Remapping node types based on dictionary mappings")
    nodes = nodes.unique("id").with_columns(
        pl.col("id").replace(nodes_dict).alias("label")
    )
    print(f"... Went from {before_len:,} to {nodes.shape[0]:,} nodes after remapping")
    print("Complete\n")
    print("Build edge file")
    print(f"... Number of predicates: {len(sem_df['PREDICATE'].unique())}")
    # remove the predicate 1532.
    sem_df = sem_df.filter(pl.col("PREDICATE") != "1532")
    print("... Initializing abbreviations for each semantic edge type")
    p_abv = {
        "ADMINISTERED_TO": "at",
        "AFFECTS": "af",
        "ASSOCIATED_WITH": "aw",
        "AUGMENTS": "ag",
        "CAUSES": "c",
        "COEXISTS_WITH": "cw",
        "COMPLICATES": "cp",
        "CONVERTS_TO": "ct",
        "DIAGNOSES": "dg",
        "DISRUPTS": "ds",
        "INHIBITS": "in",
        "INTERACTS_WITH": "iw",
        "ISA": "i",
        "LOCATION_OF": "lo",
        "MANIFESTATION_OF": "mfo",
        "MEASUREMENT_OF": "mso",  # new
        "MEASURES": "ms",  # new
        "METHOD_OF": "mo",
        "NEG_ADMINISTERED_TO": "nat",
        "NEG_AFFECTS": "naf",
        "NEG_ASSOCIATED_WITH": "naw",
        "NEG_AUGMENTS": "nag",
        "NEG_CAUSES": "nc",
        "NEG_COEXISTS_WITH": "ncw",
        "NEG_COMPLICATES": "ncp",
        "NEG_CONVERTS_TO": "nct",
        "NEG_DIAGNOSES": "ndg",
        "NEG_DISRUPTS": "nds",
        "NEG_INHIBITS": "nin",
        "NEG_INTERACTS_WITH": "niw",
        "NEG_ISA": "ni",  # new
        "NEG_LOCATION_OF": "nlo",
        "NEG_MANIFESTATION_OF": "nmfo",
        "NEG_MEASUREMENT_OF": "nmso",  # new
        "NEG_MEASURES": "nms",  # new
        "NEG_METHOD_OF": "nmo",
        "NEG_OCCURS_IN": "noi",
        "NEG_PART_OF": "npo",
        "NEG_PRECEDES": "npc",
        "NEG_PREDISPOSES": "nps",
        "NEG_PREVENTS": "npv",
        "NEG_PROCESS_OF": "npro",
        "NEG_PRODUCES": "npd",
        "NEG_STIMULATES": "nst",
        "NEG_TREATS": "nt",
        "NEG_USES": "nu",
        "NEG_higher_than": "nht",
        "NEG_lower_than": "nlt",
        "NEG_same_as": "nsa",  # new
        "OCCURS_IN": "oi",
        "PART_OF": "po",
        "PRECEDES": "pc",
        "PREDISPOSES": "ps",
        "PREVENTS": "pv",
        "PROCESS_OF": "pro",
        "PRODUCES": "pd",
        "STIMULATES": "st",
        "TREATS": "t",
        "USES": "u",
        "compared_with": "cpw",
        "higher_than": "df",
        "lower_than": "lt",
        "same_as": "sa",
    }
    print("... Initializing abbreviations for each semantic node type")
    # Run these by hand as there are few, and some don't lend themselves well to auto-generation
    sem_abv = {
        "Activities & Behaviors": "AB",
        "Anatomy": "A",
        "Compound": "C",  # don't exist in new build
        "Chemicals & Drugs": "CD",
        "Concepts & Ideas": "CI",
        "Devices": "DV",
        "Disease": "D",  # don't exist in new build
        "Disorders": "DO",
        "Genes & Molecular Sequences": "G",
        "Geographic Areas": "GA",
        "Living Beings": "LB",
        "Objects": "OB",
        "Occupations": "OC",
        "Organizations": "OR",
        "Phenomena": "PH",
        "Physiology": "PS",
        "Procedures": "PR",
    }

    print("... Initializing directions for each edge type")
    edge_dir = {
        "ADMINISTERED_TO": "",
        "AFFECTS": "",
        "ASSOCIATED_WITH": "",
        "AUGMENTS": "",
        "CAUSES": "",
        "COEXISTS_WITH": "",
        "COMPLICATES": "",
        "CONVERTS_TO": "",
        "DIAGNOSES": "",
        "DISRUPTS": "",
        "INHIBITS": "",
        "INTERACTS_WITH": "",
        "ISA": "",
        "LOCATION_OF": "",
        "MANIFESTATION_OF": "",
        "MEASUREMENT_OF": "",
        "METHOD_OF": "",
        "NOM": "",
        "OCCURS_IN": "",
        "PART_OF": "",
        "PRECEDES": "",
        "PREDISPOSES": "",
        "PREVENTS": "",
        "PROCESS_OF": "",
        "PRODUCES": "",
        "STIMULATES": "",
        "TREATS": "",
        "USES": "",
        "compared_with": ">",
        "higher_than": ">",
        "lower_than": ">",
        "same_as": ">",
    }

    nodes = nodes.with_columns(pl.col("label").replace(sem_abv).alias("abv_label"))
    print(f"...Building edge file")
    edges = sem_df[["SUBJECT_CUI", "OBJECT_CUI", "PREDICATE"]]
    edges.columns = ["start_id", "end_id", "type"]
    print(f"... Edges prior to dropping duplicates: {edges.shape[0]:,}")
    edges = edges.unique()
    print(f"... Edges after dropping duplicates: {edges.shape[0]:,}")
    # check for corrupted predicates
    assert edges.filter(pl.col("type").is_in(p_abv.keys())).shape == edges.shape
    print(f"... Mapping non-negative edges to their abbreviations")
    edges = (  # mapping abbreviations to edge file
        edges.join(
            nodes[["id", "abv_label"]], left_on="start_id", right_on="id", how="left"
        )
        .rename({"abv_label": "start_type"})
        .join(nodes[["id", "abv_label"]], left_on="end_id", right_on="id", how="left")
        .rename({"abv_label": "end_type"})
        .with_columns(
            pl.col("type").replace(p_abv).alias("type_type"),
            pl.col("type").replace(edge_dir).alias("type_dir"),
        )
    )

    print(f"... Adding PMIDs to edges")
    edges = edges.join(  # add PMIDs to triples
        pmids[["SUBJECT_CUI", "OBJECT_CUI", "PREDICATE", "PMID"]].rename(
            {
                "SUBJECT_CUI": "start_id",
                "OBJECT_CUI": "end_id",
                "PREDICATE": "type",
                "PMID": "pmid",
            }
        ),
        on=["start_id", "end_id", "type"],
        how="left",
    ).with_columns(  # look for negatives
        pl.when(pl.col("type").str.contains("NEG_"))
        .then(True)
        .otherwise(False)
        .alias("is_neg")
    )

    # generate edges with replaced negatives
    edges_replace_neg = edges.filter(
        pl.col("is_neg") == False
    ).vstack(  # stack Non-negative with negative dataframe
        edges.filter(pl.col("is_neg") == True)  # filter for items with 'neg_'
        .with_columns(
            pl.col("type").map_elements(lambda x: x[4:])
        )  # remove 'neg_' from each str
        .with_columns(
            pl.col("type")
            .replace(p_abv)
            .alias("type_type"),  # replace type_type with correct abbreviation
            pl.col("type")
            .replace(edge_dir)
            .alias("type_dir"),  # replace type direction
        )
    )

    # the following handles flags from argsparse
    if (
        args.drop_negative_relations
        and args.convert_negative_relations
        and args.include_direction
    ) or (
        args.drop_negative_relations == False
        and args.convert_negative_relations
        and args.include_direction
    ):
        # convert negative relations and include direction
        edges = edges_replace_neg.with_columns(
            (  # remake 'abbrv' column
                pl.col("start_id")
                + pl.col("type_type")
                + pl.col("type_dir")
                + pl.col("end_id")
            ).alias("abbrev"),
            (  # remake 'rev_abbrv' column
                pl.col("end_id")
                + pl.col("type_dir")
                + pl.col("type_type")
                + pl.col("start_id")
            ).alias("rev_abbrev"),
        ).drop("is_neg")

    elif (
        args.drop_negative_relations
        and args.convert_negative_relations
        and args.include_direction == False
    ) or (
        args.drop_negative_relations == False
        and args.convert_negative_relations
        and args.include_direction == False
    ):
        # convert negative relations, no direction
        edges = edges_replace_neg.with_columns(
            (  # remake 'abbrv' column
                pl.col("start_id") + pl.col("type_type") + pl.col("end_id")
            ).alias("abbrev"),
            (  # remake 'rev_abbrv' column
                pl.col("end_id") + pl.col("type_type") + pl.col("start_id")
            ).alias("rev_abbrev"),
        ).drop("is_neg")

    elif (
        args.drop_negative_relations
        and args.convert_negative_relations == False
        and args.include_direction == False
    ):
        # drop negative relation
        print("... Generating predicate abbreviations, and dropping negative relations")
        edges = edges.filter(pl.col("is_neg") == False).drop("is_neg")
        edges = edges.with_columns(
            (pl.col("start_type") + pl.col("type_type") + pl.col("end_type")).alias(
                "abbrev"
            ),
            (pl.col("end_type") + pl.col("type_type") + pl.col("start_type")).alias(
                "rev_abbrev"
            ),
        )

    elif (
        args.drop_negative_relations
        and args.convert_negative_relations == False
        and args.include_direction
    ):
        # drop negative and include direction
        print(
            f"... Generating predicate abbreviations, dropping negative relations and including direction"
        )
        edges = edges.filter(pl.col("is_neg") == False).drop("is_neg")
        edges = edges.with_columns(
            (
                pl.col("start_type")
                + pl.col("type_type")
                + pl.col("type_dir")
                + pl.col("end_type")
            ).alias("abbrev"),
            (
                pl.col("end_type")
                + pl.col("type_dir")
                + pl.col("type_type")
                + pl.col("start_type")
            ).alias("rev_abbrev"),
        )

    elif (
        args.drop_negative_relations == False
        and args.convert_negative_relations == False
        and args.include_direction
    ):
        # should be an error since you can't get direction of negatives
        # move to the top
        raise Exception("cannot add predicate directionality with negative predicates")

    elif (
        args.drop_negative_relations == False
        and args.convert_negative_relations == False
        and args.include_direction == False
    ):
        print(f"... Generating predicate abbreviations")
        edges = edges.with_columns(
            (pl.col("start_type") + pl.col("type_type") + pl.col("end_type")).alias(
                "abbrev"
            ),
            (pl.col("end_type") + pl.col("type_type") + pl.col("start_type")).alias(
                "rev_abbrev"
            ),
        )

    print("... Regroup the PMIDs")
    edges = (
        edges.unique(
            subset=["start_id", "end_id", "type"]
        )  # remove duplicates after concatenating dataframes
        .explode(
            columns="pmid"
        )  # check for more pmids and consolidate them, maybe some were associated with the 'NEG_' relation
        .group_by(
            [
                "start_id",
                "end_id",
                "type",
                "abbrev",
                "rev_abbrev",
                "start_type",
                "end_type",
                "type_type",
                "type_dir",
            ]
        )
        .agg("pmid")
    )

    print(f"... # Nodes: {nodes.shape[0]:,}")
    print(f"... # Edges: {edges.shape[0]:,}")
    # print(f"# Edges (No Neg): {edges.filter(pl.col('is_neg')==False).shape[0]:,}")
    print(f"... # Edges (Replace Neg):{edges_replace_neg.shape[0]:,}")
    print(f"... Saving Nodes to disk")
    nodes.write_parquet(
        os.path.join("../data/", f"nodes_{args.semmed_version}.parquet")
    )
    print(f"... Saving Edges to disk")
    edges.write_parquet(
        os.path.join("../data/", f"edges_{args.semmed_version}.parquet")
    )

    print('Completed "01_build_hetnet_polars.py"')


if __name__ == "__main__":
    main(parse_args())
