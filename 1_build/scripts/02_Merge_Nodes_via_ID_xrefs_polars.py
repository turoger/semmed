# Code adapting 02-Merge_nodes_via_ID_xrefs jupyter notebook.
# Meant as a script to download everything without going through the notebook

import argparse
import os
import pickle
import sys
import urllib
from collections import Counter, defaultdict

import networkx as nx
import polars as pl
from tqdm import tqdm
from wikidataintegrator import wdi_core

sys.path.append("../tools")
import load_umls


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Merge Nodes via ID Cross References. 
        Use this script to import relevant documents for processing""",
        usage="02_Merge_nodes_via_ID_xrefs_polars.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-d",
        "--dc_date",
        default="20220822",
        type=str,
        help="version date, a string, for DrugCentral dump",
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
    # load data
    print("Running 02_Merge_Nodes_via_ID_xrefs.py")
    print(f"-- dc_date (DrugCentral Date): {args.dc_date}")
    print(f"-- semmed_version (SemMed Version): {args.semmed_version}\n")
    print("Importing DrugCentral information for Gold Standard and Add Compound Names")
    print("... Loading drugcentral_rel dataframe")
    rels = pl.read_csv(f"../data/drugcentral_rel_{args.dc_date}.csv")
    print("... Loading drugcentral_ids dataframe")
    dc_ids = pl.read_csv(f"../data/drugcentral_ids_{args.dc_date}.csv")
    print("... Loading drugcentral_syn dataframe")
    pref = (
        pl.read_csv(f"../data/drugcentral_syn_{args.dc_date}.csv")
        .rename({"id": "struct_id"})
        .filter(pl.col("preferred_name") == 1)
        .unique(subset="struct_id")
    )
    struct_id_to_name = dict(zip(pref["struct_id"].to_list(), pref["name"].to_list()))
    rels = rels.with_columns(
        pl.col("struct_id").replace(struct_id_to_name).alias("c_name")
    )
    assert (
        rels.shape[0]
        == rels.with_columns(pl.col("c_name").is_not_null())["c_name"].sum()
    ), "Some concept names are missing"

    #### Map compounds in Semmed DB to MeSH
    print("Mapping compounds in semmed to MeSH")
    nodes = pl.read_parquet(
        f"../data/nodes_{args.semmed_version}.parquet"
    )  # import semmed nodes file
    umls_to_mesh = pickle.load(
        open("../data/UMLS-CUI_to_MeSH-Descripctor.pkl", "rb")
    )  # get UMLS-CUI to MeSH direct maps
    umls_to_mesh_1t1 = {
        k: v[0] for k, v in umls_to_mesh.items() if len(v) == 1
    }  # create the direct map dictionary
    nodes = nodes.with_columns(
        pl.col("id").replace(umls_to_mesh_1t1).alias("mesh_id")
    )  # relabel the UMLS ids with MeSH ids
    drugs = nodes.filter(pl.col("label") == "Chemicals & Drugs")
    print(
        f"... {drugs['mesh_id'].is_not_null().sum()/drugs.shape[0]:.2%} of Drug IDs mapped via MeSH"
    )
    print(
        f"... {drugs['mesh_id'].is_not_null().sum():,} of {drugs.shape[0]:,} mapped to {drugs.filter(pl.col('mesh_id').is_not_null())['mesh_id'].n_unique():,} Unique MSH ids."
    )

    num_drugs = drugs.filter(pl.col("mesh_id").is_not_null())["id"].n_unique()
    msh_compress_drugs = drugs.filter(pl.col("mesh_id").is_not_null())[
        "mesh_id"
    ].n_unique()

    print(
        f"... {(num_drugs-msh_compress_drugs)/num_drugs:.2%} Reduction in Drugs by using MSH synonmyms {num_drugs:,} --> {msh_compress_drugs:,}"
    )
    #### Use UMLS MeSH mappings and Mappings from DrugCentral to ensure Maximum overlap
    print("Generate MeSH to DrugCentral map")
    # get dataframe of specific id_types
    dc_maps = dc_ids.filter(
        pl.col("id_type").is_in(
            ["MESH_DESCRIPTOR_UI", "MESH_SUPPLEMENTAL_RECORD_UI", "UMLSCUI"]
        )
    ).with_columns(
        pl.col("id").cast(str), pl.col("struct_id").cast(str)
    )  # turn id and struct_id to string

    # create a dictionary of drugs and their neighbors, bi-directional
    print("... use drug identifiers to find nearest neighbor mesh identifiers")
    drug_adj_list = dict(
        zip(  # struct:[identifier] mapping
            dc_maps.group_by("struct_id").agg(["identifier"])["struct_id"].to_list(),
            dc_maps.group_by("struct_id").agg(["identifier"])["identifier"].to_list(),
        )
    )

    drug_adj_list.update(
        dict(
            zip(  # identifier:[struct] map
                dc_maps.group_by("identifier").agg("struct_id")["identifier"].to_list(),
                dc_maps.group_by("identifier").agg("struct_id")["struct_id"].to_list(),
            )
        )
    )

    umls_to_mesh_df = pl.DataFrame(
        {"umls": umls_to_mesh.keys(), "mesh": umls_to_mesh.values()}
    ).explode("mesh")
    print(f"... umls to mesh mapping rows: {umls_to_mesh_df.shape[0]:,}")
    umls_to_mesh_drugs = umls_to_mesh_df.filter(
        pl.col("umls").is_in(drugs["id"].unique())
    )
    print(
        f"... umls to mesh mapping rows after filtering for drug ids: {umls_to_mesh_drugs.shape[0]:,}"
    )

    umls_set = set(drugs["id"].to_list()).union(
        set(dc_maps.filter(pl.col("id_type") == "UMLSCUI")["identifier"])
    )
    mesh_set = set(umls_to_mesh_df["mesh"].to_list()).union(
        set(
            dc_maps.filter(
                pl.col("id_type").is_in(
                    ["MESH_DESCRIPTOR_UI", "MESH_SUPPLEMENTAL_RECORD_UI"]
                )
            )["identifier"]
        )
    )
    assert (
        len(umls_set.intersection(mesh_set)) == 0
    ), "UMLS and MESH identifiers have at least one shared identifier; there should be zero."
    print("... build drug identifier nearest neighbors adjacency matrix")
    drug_adj_list_df = pl.DataFrame(
        {"k": drug_adj_list.keys(), "v": drug_adj_list.values()}
    ).explode(columns="v")
    # find each umls identifier in the dictionary and add the mesh to the list.
    # find each mesh identifier in the dict and add the umls to the list
    drug_adj_list_df = (
        drug_adj_list_df.vstack(umls_to_mesh_drugs.rename({"umls": "k", "mesh": "v"}))
        .vstack(umls_to_mesh_drugs.rename({"mesh": "k", "umls": "v"})[["k", "v"]])
        .group_by("k")
        .agg("v")
    )
    drug_adj_list = dict(
        zip(drug_adj_list_df["k"].to_list(), drug_adj_list_df["v"].to_list())
    )
    # Ensure that all Struct IDs from DrugCentral make it into the subnets (even if no xrefs)
    for struct_id in (
        rels.filter(pl.col("relationship_name") == "indication")["struct_id"]
        .unique()
        .cast(str)
        .to_list()
    ):
        if struct_id not in drug_adj_list.keys():
            drug_adj_list[struct_id] = [struct_id]
        else:
            drug_adj_list[struct_id] += [struct_id]

    # identify nearest neighbors using a breadth first search
    def get_subnets2(adj_list):
        all_identifiers = set(adj_list.keys())
        subnets = defaultdict(set)
        graph = nx.Graph(adj_list)

        for cui in tqdm(all_identifiers):
            subnets[cui] = set(nx.bfs_tree(graph, cui, depth_limit=1))
        return subnets

    print("... process adjacency matrix to calculate nearest neighbors")
    subnets2 = get_subnets2(drug_adj_list)
    # get a count of all MeSH terms, the greater the count, the higher the term priority
    mesh_counts = (
        umls_to_mesh_df["umls"].to_list()
        + umls_to_mesh_df["mesh"].to_list()
        + dc_maps["identifier"].to_list()
        + dc_maps["struct_id"].unique().to_list()
    )
    mesh_counts = Counter(mesh_counts)
    # Based on the MeSH term counts, pick the nearest neighbor with the highest count
    print("... picking nearest neighbor")
    rekeyed_subnet2 = (
        pl.DataFrame(
            {"k": subnets2.keys(), "v": [list(i) for i in subnets2.values()]}
        )  # create df from dictionary, turn everything to lists
        # .head()
        .with_columns(
            pl.col("k").map_elements(lambda x: [x])
        )  # turn 'k' type to list so we can concatenate
        .with_columns(
            pl.struct(["k", "v"])  # concatenate the two columns
            .map_elements(lambda x: (x["v"] + x["k"]))
            .list.unique()  # get unique items in list
            .map_elements(  # extract highest count child
                lambda x: sorted(
                    x,
                    key=lambda i: (mesh_counts[i], i in mesh_set, i in umls_set),
                    reverse=True,
                )
            )
            .alias("v"),
            pl.col("v")
            .map_elements(lambda x: [mesh_counts[i] for i in x])
            .alias("new_ct"),
        )
        .with_columns(
            pl.col("v").map_elements(lambda x: x[0]).alias("new"),
            pl.col("new_ct")
            .map_elements(lambda x: (sum(x) / len(x)) == 1)
            .alias("no_diff"),
        )
    )

    rekeyed_subnet3 = rekeyed_subnet2.filter(  # removes ties, anything with a tie doesn't make it into the dict
        pl.col("no_diff") == False
    ).explode(
        "k"
    )
    # map unmapped items to original
    unmapped_drugs = (
        rekeyed_subnet2.filter(pl.col("no_diff") == True).explode("k").unique("k")
    )

    unmapped_drug_dict = drugs.filter(pl.col("id").is_in(unmapped_drugs["k"]))[
        ["id", "mesh_id"]
    ]
    unmapped_drug_dict = dict(
        zip(unmapped_drug_dict["id"].to_list(), unmapped_drug_dict["mesh_id"].to_list())
    )
    # create final dict
    final_drug_map = dict(
        zip(rekeyed_subnet3["k"].to_list(), rekeyed_subnet3["new"].to_list())
    )
    print(f"... final drug map size prior to adding unmapped: {len(final_drug_map):,}")
    final_drug_map.update(unmapped_drug_dict)
    print(f"... final drug map size after adding unmapped: {len(final_drug_map):,}")

    # some tests to ensure a couple items are mapped correctly
    assert (
        final_drug_map["C0039608"] == "D013739"
    ), "Testosterone is not mapped properly; should be mapped to Testosterone."
    assert (
        final_drug_map["C0037512"] == "D012971"
    ), "Gold sodium thiosulfate is not mapped properly; should be mapped to Mesna, a sulfhydryl compound."
    assert (
        final_drug_map["C0040989"] == "D014273"
    ), "Triflupromazine is not mapped properly; should be Triflupromazine."
    print("... export drug to mesh mapping to '../data/drug_merge_map.pkl'")
    pickle.dump(final_drug_map, open("../data/drug_merge_map.pkl", "wb"))

    #### 4. Map all the compounds and check
    print("Mapping all the compounds to Drugs")
    # Some items won't necessarily be mappable, so use original ID
    drugs = drugs.with_columns(
        pl.col("id").replace(final_drug_map).alias("new_id")
    ).with_columns(  # update id_source
        pl.when(pl.col("new_id").is_in(mesh_set))
        .then(pl.lit("MeSH"))
        .otherwise(pl.lit("UMLS"))
        .alias("id_source")
    )

    # Map the Gold Standard indications as well
    rels = rels.with_columns(
        pl.col("struct_id").cast(str).replace(final_drug_map).alias("compound_new_id")
    )

    print(
        f'... reduced in drug nodes by: {(drugs.shape[0]-drugs.n_unique(subset="new_id"))/drugs.shape[0]:.2%}'
    )
    print(f'... from {drugs.shape[0]:,} to {drugs.n_unique(subset="new_id"):,}')

    # get counts of conversions
    inds = rels.filter(pl.col("relationship_name") == "indication")
    drug_ids_semmed = set(drugs["new_id"].unique().to_list())
    drugs_in_inds = set(inds["compound_new_id"].unique().to_list())
    num_ind_in_semmed = len(drugs_in_inds.intersection(drug_ids_semmed))
    ind_semmed_comp = inds.filter(
        pl.col("compound_new_id").is_in(drug_ids_semmed)
    ).shape[0]

    print(
        f"... {(num_ind_in_semmed/len(drugs_in_inds)):.2%} of Drugs in DC Indications mapped ({num_ind_in_semmed:,} out of {len(drugs_in_inds):,})"
    )
    print(
        f"... {(ind_semmed_comp/len(inds)):.2%} of Indications have mappable Drugs ({ind_semmed_comp:,} out of {len(inds):,})"
    )

    #### Diseases. Use DO Slim to try and get general diseases
    print("Mapping diseases to Disease Ontology Slim IDs to generalize diseases")
    diseases = nodes.filter(pl.col("label") == "Disorders")
    print(f"... Number of diseases in dataframe: {len(diseases):,}")
    # mesh label to number of disease mappings
    dis_number = (
        diseases.group_by("mesh_id")
        .agg(["id"])
        .with_columns(pl.col("id").list.unique().list.len())
        .sort(by="id", descending=True)
    )

    # loading umls dataframe and extract snomedct_us identifiers
    conso = load_umls.open_mrconso()
    snomed_xrefs = conso.filter(
        pl.col("SAB") == "SNOMEDCT_US", pl.col("LAT") == "ENG"
    ).drop_nulls(subset=["CUI", "SCUI"])
    # add umls to mesh as k:v in a dict, add mesh to umls as k:v
    disease_ids = set(diseases["id"].unique())
    umls_to_mesh_dis = umls_to_mesh_df.filter(pl.col("umls").is_in(disease_ids))
    sub_rels = rels.drop_nulls(["snomed_conceptid", "umls_cui"]).with_columns(
        pl.col("snomed_conceptid").cast(str)
    )
    ind_snomed = set(rels["snomed_conceptid"])
    dis_umls = set(rels["umls_cui"]).union(disease_ids)

    dis_snomed_xrefs = snomed_xrefs.with_columns(pl.col("SCUI").cast(int)).filter(
        pl.col("CUI").is_in(dis_umls), pl.col("SCUI").is_in(ind_snomed)
    )
    # Convert the snomed concept ids to string since they're strings the adj_list
    a = umls_to_mesh_dis.rename({"umls": "k", "mesh": "v"})[["k", "v"]]
    b = umls_to_mesh_dis.rename({"umls": "v", "mesh": "k"})[["k", "v"]]
    c = sub_rels.rename({"umls_cui": "k", "snomed_conceptid": "v"})[["k", "v"]]
    d = sub_rels.rename({"snomed_conceptid": "k", "umls_cui": "v"})[["k", "v"]]
    # Make sure to get mesh to CUI maps for the new cuis picked up via drugcentral
    e = pl.DataFrame({"k": umls_to_mesh_1t1.keys(), "v": umls_to_mesh_1t1.values()})
    f = pl.DataFrame({"k": umls_to_mesh_1t1.values(), "v": umls_to_mesh_1t1.keys()})

    g = (
        dis_snomed_xrefs[["CUI", "SCUI"]]
        .rename({"CUI": "k", "SCUI": "v"})
        .with_columns(pl.col("v").cast(str))[["k", "v"]]
    )
    h = (
        dis_snomed_xrefs[["CUI", "SCUI"]]
        .rename({"CUI": "v", "SCUI": "k"})
        .with_columns(pl.col("k").cast(str))[["k", "v"]]
    )
    dis_adj_df = pl.concat([a, b, c, d, e, f, g, h])
    print(f"... Before removing duplicate diseases: {dis_adj_df.shape[0]:,}")
    print(f"... After removing duplicate diseases: {dis_adj_df.unique().shape[0]:,}")

    #### DO Slim Integration
    print("Get DO Slim Ids from WikiData via WikiDataIntegrator")
    print("... query Wikidata for doid to umlscui's")
    query_text = """
    select ?doid ?umlscui

    WHERE
    {
        ?s wdt:P699 ?doid .
        ?s wdt:P2892 ?umlscui .
    }
    """
    print("... processing wikidata results")
    result = wdi_core.WDItemEngine.execute_sparql_query(query_text, as_dataframe=True)
    result.to_csv("../data/doid-to-umls.csv", index=False)
    doid_to_umls = result.set_index("doid")["umlscui"].to_dict()

    # download the relevant disease-ontology files and read them as csv
    for filename in ["xrefs-prop-slim.tsv", "slim-terms-prop.tsv"]:
        if not os.path.exists(file := os.path.join("../data", filename)):
            urllib.request.urlretrieve(
                f"https://raw.githubusercontent.com/mmayers12/disease-ontology/gh-pages/data/{filename}",
                filename=file,
            )
    slim_xref = pl.read_csv("../data/xrefs-prop-slim.tsv", separator="\t")
    do_slim = pl.read_csv("../data/slim-terms-prop.tsv", separator="\t")

    resources = [
        "SNOMEDCT_US_2022_09_01",
        "UMLS",
        "MESH",
        "SNOMEDCT",
        "SNOMEDCT_US_2021_09_01",
        "SNOMEDCT_US_2023_02_28",
        "SNOMEDCT_US_2022_07_31",
        "SNOMEDCT_US_2021_07_31",
        "SNOMEDCT_US_2022_12_31",
    ]
    useful_xref = slim_xref.filter(pl.col("resource").is_in(resources))
    do_slim = (
        do_slim.with_columns(pl.col("subsumed_id").replace(doid_to_umls).alias("cui"))
        .filter(pl.col("cui").is_in(list(doid_to_umls.values())))
        .drop_nulls("cui")
    )
    print("... creating an adjacency matrix of diseases and their nearest neighbors")
    # create a dictionary of doid to resource and subsumed to cui to do the same nearest neighbor mapping as above
    # concatenate in the useful xrefs from DO and the DO slim
    i = useful_xref.rename({"doid_code": "k", "resource_id": "v"})[["k", "v"]]
    j = useful_xref.rename({"doid_code": "v", "resource_id": "k"})[["k", "v"]]
    k = do_slim.rename({"subsumed_id": "k", "cui": "v"})[["k", "v"]]
    l = do_slim.rename({"subsumed_id": "v", "cui": "k"})[["k", "v"]]
    dis_adj_df = pl.concat([dis_adj_df, i, j, k, l]).unique()
    do_slim_terms = dict(
        zip(do_slim["slim_id"].to_list(), do_slim["slim_name"].to_list())
    )
    slim_ids = set(do_slim_terms.keys())

    #### Make final map for Diseases and map them
    print("... find disease nearest neighbors")
    dis_adj_df = dis_adj_df.unique().group_by("k").agg("v")
    dis_adj_list = dict(zip(dis_adj_df["k"].to_list(), dis_adj_df["v"].to_list()))
    # find the nearest neighbors
    dis_subnets = get_subnets2(dis_adj_list)
    umls_set = set(diseases["id"].drop_nulls().unique().to_list()).union(
        set(rels["umls_cui"].drop_nulls().unique().to_list())
    )
    umls_to_val = {u: 9999999 - int(u[1:]) for u in umls_set}
    # get the mesh count and store the counts in a dictionary
    mesh_counts = (
        umls_to_mesh_df["umls"].to_list()
        + umls_to_mesh_df["mesh"].to_list()
        + rels.filter(pl.col("umls_cui").is_in(umls_to_mesh_1t1.keys()))
        .with_columns(pl.col("umls_cui").replace(umls_to_mesh_1t1))["umls_cui"]
        .to_list()
        + rels.filter(~pl.col("umls_cui").is_in(umls_to_mesh_1t1.keys()))[
            "umls_cui"
        ].to_list()
    )
    mesh_counts = Counter(mesh_counts)
    rekeyed_dis_subnets = dict()
    for v in dis_subnets.values():
        # If a disease was consolidated under DO-SLIM, take the slim ID and name
        if v & slim_ids:
            new_key = (v & slim_ids).pop()
            rekeyed_dis_subnets[new_key] = v
        else:
            # First take ones in the mesh, then by the highest number of things it consolidated
            # Then take the lowest numbered UMLS ID...
            sort_sub = sorted(
                list(v),
                key=lambda k: (
                    k in mesh_set,
                    mesh_counts[k],
                    k in umls_set,
                    umls_to_val.get(k, 0),
                ),
                reverse=True,
            )
            new_key = sort_sub[0]
            rekeyed_dis_subnets[new_key] = v
    print("... Make final map for Diseases and map disease entities")
    # Final map is just inverse of the subnets dict
    final_dis_map = dict()

    for k, v in rekeyed_dis_subnets.items():
        for val in v:
            final_dis_map[val] = k
    print("... remap diseases to new ids")
    # remap only items that exist in final_dis_map, otherwise add 'id' as 'new_id'
    diseases = (
        diseases.filter(pl.col("id").is_in(final_dis_map.keys()))
        .with_columns(pl.col("id").replace(final_dis_map).alias("new_id"))
        .vstack(
            diseases.filter(~pl.col("id").is_in(final_dis_map.keys())).with_columns(
                pl.col("id").alias("new_id")
            )
        )
    )

    # See how many instances of diseases mapped to 1 mesh ID had their ID changed through
    # SNOMED and DO-SLIM consolidation
    print(
        f"... {diseases.drop_nulls('mesh_id').filter(~pl.col('mesh_id').is_in(pl.col('new_id')))['id'].n_unique():,} original CUIs"
    )
    print(
        f"... {diseases.drop_nulls('mesh_id').filter(~pl.col('mesh_id').is_in(pl.col('new_id')))['mesh_id'].n_unique():,} Mapped to MeSH IDs"
    )
    print(
        f"... {diseases.drop_nulls('mesh_id').filter(~pl.col('mesh_id').is_in(pl.col('new_id')))['new_id'].n_unique():,} Consolidated unique entitites"
    )

    def dis_source_map(x):
        if x in mesh_set:
            return "MeSH"
        elif x in umls_set:
            return "UMLS"
        elif x.startswith("DOID:"):
            return "DO-Slim"
        else:
            # Just in case there's a problem...
            return "Uh-Oh"

    diseases = diseases.with_columns(
        pl.col("new_id").map_elements(lambda x: dis_source_map(x)).alias("id_source")
    )
    pickle.dump(final_dis_map, open("../data/disease_merge_map.pkl", "wb"))
    print(
        f'... {(diseases.shape[0] - diseases["new_id"].n_unique()) / diseases.shape[0]:.2%} Reduction in Diseases. ({diseases.shape[0]:,} -> {diseases["new_id"].n_unique():,})'
    )
    rels = (
        rels.filter(pl.col("umls_cui").is_in(final_dis_map.keys()))
        .with_columns(pl.col("umls_cui").replace(final_dis_map).alias("disease_new_id"))
        .vstack(
            rels.filter(~pl.col("umls_cui").is_in(final_dis_map.keys())).with_columns(
                pl.col("umls_cui").replace(final_dis_map).alias("disease_new_id")
            )
        )
    )
    inds = rels.filter(pl.col("relationship_name") == "indication")
    disease_ids_semmed = set(diseases["new_id"].to_list())
    diseases_in_inds = set(inds["disease_new_id"].drop_nulls().to_list())
    num_ind_in_semmed = len(diseases_in_inds.intersection(disease_ids_semmed))
    ind_semmed_comp = inds.filter(
        pl.col("disease_new_id").is_in(disease_ids_semmed)
    ).shape[0]
    print(
        f"... {num_ind_in_semmed/len(diseases_in_inds):.2%} of diseases in DC Indications mapped: {num_ind_in_semmed:,} of {len(diseases_in_inds):,}"
    )
    print(
        f"... {(ind_semmed_comp/len(inds)):.2%} of Indications have mappable diseases: {ind_semmed_comp:,} of {len(inds):,}"
    )
    inds_dd = inds.unique(["compound_new_id", "disease_new_id"])
    new_cid = set(drugs["new_id"].unique().to_list())
    new_dids = set(diseases["new_id"].unique().to_list())
    inds_in_semmed = inds_dd.filter(
        pl.col("compound_new_id").is_in(new_cid)
        & pl.col("disease_new_id").is_in(new_dids)
    )
    print(
        f"... {len(inds_in_semmed)/len(inds_dd):.2%} of indications have both compound and disease mappings ({len(inds_in_semmed):,} of {len(inds_dd):,})"
    )

    #### Add in Dates for indications ####
    print("Get dates for drug approval indications")
    app = pl.read_csv(f"../data/drugcentral_approvals_{args.dc_date}.csv")
    app = (
        app.unique("approval")  # Remove NaN values
        .sort("approval", descending=True)  # Put the earliest approval_date first
        .group_by("struct_id")  # Group by the compound's id
        .first()  # And select the first instance of that id
        .rename({"approval": "approval_date"})
    )
    rels = rels.join(app[["struct_id", "approval_date"]], on="struct_id", how="left")
    # get year date, not month date
    rels = rels.filter(~pl.col("approval_date").is_null()).with_columns(
        pl.col("approval_date")
        .map_elements(lambda x: x.split("-")[0])
        .alias("approval_year")
    )

    #### Rebuild the Nodes to new ID mappings
    print("Rebuilding nodes to appropriate new ID maps")
    all_umls = set(nodes["id"])
    umls_set = all_umls.union(
        set(dc_maps.filter(pl.col("id_type") == "UMLSCUI")["identifier"])
    ).union(set(rels["umls_cui"]))

    def get_source(cid):
        if cid in mesh_set:
            return "MeSH"
        elif cid in umls_set:
            return "UMLS"
        elif cid.startswith("DOID:"):
            return "DO-Slim"
        else:
            return "problem..."

    print("... export node mappings")
    pickle.dump(umls_set, open("../data/umls_id_set.pkl", "wb"))
    pickle.dump(mesh_set, open("../data/mesh_id_set.pkl", "wb"))

    new_nodes = nodes.filter(~pl.col("label").is_in(["Chemicals & Drugs", "Disorders"]))
    new_nodes = new_nodes.with_columns(
        pl.col("mesh_id").fill_null(new_nodes["id"]).alias("new_id")
    ).with_columns(
        pl.col("new_id").map_elements(lambda x: get_source(x)).alias("id_source")
    )

    drug_dis = drugs.vstack(diseases)
    curr_map = dict(zip(drug_dis["id"].to_list(), drug_dis["new_id"].to_list()))
    problem_new_id = (
        drug_dis.group_by("new_id")
        .agg("label")
        .with_columns(pl.col("label").list.unique())
        .filter(pl.col("label").list.len() > 1)["new_id"]
    )
    new_map = dict(zip(drug_dis["new_id"].to_list(), drug_dis["label"].to_list()))
    new_map.update(
        {
            "D004967": "Chemicals & Drugs",
            "D013566": "Chemicals & Drugs",
        }
    )
    drug_dis = drug_dis.with_columns(
        pl.col("new_id")
        .replace(
            new_map,
        )
        .alias("label")
    )
    assert (
        rels.filter(
            pl.col("disease_new_id").is_in(
                [
                    "D013566",
                    "D004967",
                ]
            )
        ).shape[0]
        == 0
    ), "No issues in updating diseases wih multiple label mappings."

    new_nodes = new_nodes.vstack(drug_dis).sort("label")
    problem_ids = (
        new_nodes.group_by("new_id")
        .agg("label")
        .with_columns(pl.col("label").list.unique())
        .with_columns(
            pl.col("label").list.len().alias("len_label"),
        )
        .filter(pl.col("len_label") > 1)["new_id"]
    )
    print(f"... {len(problem_ids)} remaining identifiers with multiple labels")

    #### fix other node-type conflicts
    edges = pl.read_parquet(f"../data/edges_{args.semmed_version}.parquet").rename(
        {"start_id": "h_id", "end_id": "t_id", "type": "edge"}
    )

    cui_counts = (
        edges["h_id"]
        .value_counts()
        .rename({"h_id": "k"})
        .vstack(edges["t_id"].value_counts().rename({"t_id": "k"}))
    )

    cui_counts = dict(zip(cui_counts["k"].to_list(), cui_counts["counts"].to_list()))
    # convert conflicting `new_id` to their old `id` and get the semmantic type
    grpd = (
        new_nodes.filter(pl.col("new_id").is_in(problem_ids))
        .group_by("new_id")
        .agg(["id", "label"])
    )
    remap = dict()
    remap_lab = dict()

    for new_id in tqdm(
        temp := grpd["new_id"].to_list(), total=len(temp)
    ):  # note, new_id and mesh_id are 100% the same after group_by
        # Get all the labels and counts for those labels
        labels = (
            grpd.filter(pl.col("new_id") == new_id)["label"].list.unique()[0].to_list()
        )
        counts = Counter(grpd.filter(pl.col("new_id") == new_id)["label"][0].to_list())
        # Sort the by the Number of different nodes mapped to that label
        labels = sorted(labels, key=lambda l: counts[l], reverse=True)

        # Chemicals and Drugs and Diseases have higher priorities in the context of machine learning
        # So any item that could be either of those types will be set to them automatically.
        drug_or_dis = False
        # Select the Chemicals & Drugs nodes to have the MeSH ID if possible
        if "Chemicals & Drugs" in labels:
            labels.remove("Chemicals & Drugs")
            curr_label = grpd.filter(
                pl.col("label").list.contains("Chemicals & Drugs"),
                pl.col("new_id") == new_id,
            )["id"][0].to_list()
            drug_or_dis = True
            for c in curr_label:
                remap[c] = new_id  # old entity to new entity map
                remap_lab[c] = "Chemicals & Drugs"  # old entity to new label map
        # Otherwise, elect the Disorders nodes to have the MeSH ID if possible
        elif "Disorders" in labels:
            labels.remove("Disorders")
            curr_label = grpd.filter(
                pl.col("label").list.contains("Disorders"), pl.col("new_id") == new_id
            )["id"][0].to_list()
            drug_or_dis = True
            for c in curr_label:
                remap[c] = new_id
                remap_lab[c] = "Disorders"

        # Finally assign a merged CUI based on edge counts
        # for i, label in enumerate(labels):
        #     curr_label = grpd.filter(
        #         pl.col("label").list.contains(label), pl.col("new_id") == new_id
        #     )["id"][0].to_list()
        curr_label = grpd.filter(pl.col("new_id") == new_id)["id"][0].to_list()
        # Give highest counts of MeSH nodes, if not already assigned to a Drug or Disease
        # if i == 0 and not drug_or_dis:
        if not drug_or_dis:
            new_cui = new_id
        else:
            # For types that won't get a MeSH ID,
            # get the CUI that has largest number of instances in the edges
            new_cui = sorted(
                curr_label, key=lambda v: cui_counts.get(v, 0), reverse=True
            )[0]
            new_cui_lab = new_nodes.filter(pl.col("id") == new_cui)["label"][0]
        for c in curr_label:
            remap[c] = new_cui
            try:
                remap_lab[c] = new_cui_lab
            except:
                remap_lab[c] = None

    # Get old mappings, and update them with new mappings
    curr_map = dict(zip(new_nodes["id"].to_list(), new_nodes["new_id"].to_list()))
    curr_map.update(remap)

    curr_lab_map = dict(zip(new_nodes["id"].to_list(), new_nodes["label"].to_list()))
    curr_lab_map.update(remap_lab)

    # assign new mappings
    new_nodes = new_nodes.with_columns(
        pl.col("id").replace(curr_map).alias("new_id"),
        pl.col("id").replace(curr_lab_map).alias("label"),
    ).drop_nulls(["new_id", "label"])

    # check mappings for multiple mappings
    assert (
        new_nodes.group_by("new_id")
        .agg("label")
        .with_columns(pl.col("label").list.unique())
        .with_columns(pl.col("label").list.len().alias("len_label"))
        .filter(pl.col("len_label") > 1)
        .shape[0]
        == 0
    ), "There are still some nodes with multiple label mappings."

    num_old_ids = new_nodes["id"].n_unique()
    num_new_ids = new_nodes["new_id"].n_unique()

    print(
        f"Nodes reduced by {(num_old_ids-num_new_ids)/num_old_ids:.2%}; from {num_old_ids:,} -> {num_new_ids:,}."
    )

    new_nodes = new_nodes.with_columns(
        pl.col("new_id").map_elements(lambda x: get_source(x)).alias("id_source")
    )
    new_nodes = new_nodes.filter(pl.col("id_source") != "problem...")
    # create cui to name mappings
    cui_to_name = dict(zip(nodes["id"].to_list(), nodes["name"].to_list()))  # {id:name}
    cui_to_name = {
        **cui_to_name,
        **dict(zip(rels["umls_cui"].to_list(), rels["concept_name"].to_list())),
    }  # {umls_cui: concept_name}
    cui_to_name = {
        **cui_to_name,
        **dict(zip(rels["compound_new_id"].to_list(), rels["c_name"].to_list())),
    }  # {compound_new_id:c_name}

    # create mesh to name mappings
    msh_to_name = pickle.load(open("../data/MeSH_DescUID_to_Name.pkl", "rb"))
    msh_to_name = {
        **msh_to_name,
        **pickle.load(open("../data/MeSH_id_to_name_via_UMLS.pkl", "rb")),
    }
    id_to_name = {**struct_id_to_name, **do_slim_terms, **cui_to_name, **msh_to_name}
    id_to_name = {str(k): str(v) for k, v in id_to_name.items()}
    # All new IDs should have a mapped name
    assert set(new_nodes["new_id"].to_list()).issubset(
        set(id_to_name.keys())
    ), "Some new IDs do not have a mapped name"
    # relabel column as 'name'
    new_nodes = new_nodes.with_columns(
        pl.col("new_id").replace(id_to_name).alias("name")
    )
    pickle.dump(id_to_name, open("../data/all_ids_to_names.pkl", "wb"))
    final_node_map = dict(zip(new_nodes["id"].to_list(), new_nodes["new_id"].to_list()))

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
    new_nodes = new_nodes.with_columns(
        pl.col("label").replace(sem_abv).alias("abv_label")
    )
    final_node_label_map = dict(
        zip(new_nodes["id"].to_list(), new_nodes["abv_label"].to_list())
    )

    #### Map all the edges remaining
    print("Map re-mapped nodes to the edges")
    edges = edges.with_columns(
        pl.col("h_id").replace(final_node_map).alias("h_id"),
        pl.col("t_id").replace(final_node_map).alias("t_id"),
        pl.col("h_id").replace(final_node_label_map).alias("start_type"),
        pl.col("t_id").replace(final_node_label_map).alias("end_type"),
    )
    print(
        "... Mapping Node Ids to the edges. Duplicated edges will be deleted. \
    This can take up to 30 minutes"
    )
    num_before = len(edges)
    # remove duplicate edges and combine the pmids
    edges = (
        edges.filter(
            pl.col("h_id").is_in(new_nodes["new_id"]),
            pl.col("t_id").is_in(new_nodes["new_id"]),
        )
        .explode("pmid")
        .group_by([i for i in edges.columns if i != "pmid"])
        .agg("pmid")
        .with_columns(
            pl.col("pmid").list.unique(),
            pl.col("pmid").list.unique().list.len().alias("n_pmids"),
        )
    )

    num_after = len(edges)

    print(f"... There are: {num_before:,} Edges before node consolidation")
    print(f"... There are: {num_after:,} Edges after node consolidation")
    print(f"... A {((num_before - num_after) / num_before):.3%} reduction in edges")

    # difference between edge head and tail ids and the new node ids
    assert (
        len(
            set(edges["h_id"].to_list())
            .union(set(edges["t_id"].to_list()))
            .difference(set(new_nodes["new_id"].to_list()))
        )
        == 0
    ), "There are some unmapped nodes in the edge file"

    #### Save files to the network
    print("Saving Network files (consolidated nodes and edges)")
    # export edge file
    edges.sort("edge").write_parquet(
        f"../data/edges_{args.semmed_version}_consolidated.parquet"
    )
    # replace old ids in the nodes, sort and write nodes to disk
    new_nodes[["new_id", "name", "label", "abv_label", "id_source"]].rename(
        {"new_id": "id"}
    ).unique("id").sort("label").write_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated.parquet"
    )
    # sort labels and write edges to disk
    pickle.dump(final_node_map, open("../data/node_id_merge_map.pkl", "wb"))

    #### Save relationship files for ML gold standard
    print('Saving a seperate "indications" file for a Machine Learning gold standard')
    # Do some rennaming of the columns before saving
    rels = rels.rename(
        {
            "c_name": "compound_name",
            "concept_name": "disease_name",
            "compound_new_id": "compound_semmed_id",
            "disease_new_id": "disease_semmed_id",
        }
    )
    # Keep indications for gold standard
    # Keep duplicates in Rels just in case they're useful. Indications file should have no duplicates
    # Indications
    rels.filter(pl.col("relationship_name") == "indication").unique(
        ["compound_semmed_id", "disease_semmed_id"]
    ).write_parquet("../data/indications_nodemerge.parquet")
    rels.write_parquet("../data/gold_standard_relationships_nodemerge.parquet")

    print("Complete processing 02_Merge_Nodes_via_ID_xrefs.py\n")


if __name__ == "__main__":
    main(parse_args())
