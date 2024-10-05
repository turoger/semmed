import argparse
import logging
import sys

import pandas as pd
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
        description=r"""Filter low abundance edges. 
        Use this script to remove low abundance edges in SemMedDB.""",
        usage="04_filter_low_abundance_edges_polars.py [<args>] [-h | --help]",
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
    logger.info(f"Running 04_filter_low_abundance_edges.py")
    logger.info("... Loading data")
    #### Filter low abundance edges
    nodes = pl.read_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated_condensed.parquet"
    )
    edges = pl.read_parquet(
        f"../data/edges_{args.semmed_version}_consolidated_condensed.parquet"
    )

    comp = len(nodes.filter(pl.col("label") == "Chemicals & Drugs"))
    dis = len(nodes.filter(pl.col("label") == "Disorders"))
    logger.info(
        f"... {comp:,} Compounds x {dis:,} Diseases = {comp*dis:,} Compound-Disease combinations"
    )
    # get edge type counts, sorted by count
    counts = edges["r"].value_counts()
    logger.info(f"... There are {counts.shape[0]:,} unique edge types")

    logger.info("... Filtering low abundance edges")
    cutoff = 0.001
    logger.info(f"... Total Number of edges: {len(edges):,}")
    logger.info(
        f"... Applying a {cutoff:%} cutoff to edges. Only edges greater than {cutoff*len(edges):,} will be retained."
    )
    logger.info(
        f"... Number of edge types with this cutoff: {len((filt_counts:=counts.filter(pl.col('count')>(cutoff*len(edges))))):,}"
    )
    logger.info(
        f"... Number of edges that remain with this cutoff: {len(filt_edges:=edges.filter(pl.col('r').is_in(filt_counts['r']))):,}"
    )

    logger.info("... Get the nodes from the filtered edge dataframe")
    # get edge_ids from filtered edges
    edge_ids = set(filt_edges["h_id"].to_list()).union(
        set(filt_edges["t_id"].to_list())
    )
    logger.info(f"... Number of unique nodes in filtered edges: {len(edge_ids):,}")

    node_ids = set(nodes["id"].unique().to_list())
    logger.info(f"... Number of unique nodes in nodes: {len(node_ids):,}")
    not_in_edges = node_ids - edge_ids
    logger.info(f"... Number of nodes not in edges: {len(not_in_edges):,}")
    nodes = nodes.filter(pl.col("id").is_in(not_in_edges).not_())
    assert len(nodes) == len(edge_ids), "Number of nodes and edges should be the same"
    logger.info(f"... Number of nodes in nodes after filtering: {len(nodes):,}")

    filt_edge_sz = filt_edges.shape[0]
    filt_edges = filt_edges.filter(pl.col("h_id") != pl.col("t_id"))

    logger.info(
        f"... Number of edges prior to removal of self-referential edges: {filt_edge_sz:,}"
    )
    logger.info(
        f"... Number of edges after removal of self-referential edges: {filt_edges.shape[0]:,}"
    )

    logger.info("... remove overly general nodes (top most commonly occurring)")

    #### filter overly general nodes
    # These are from the 100 most common nodes, removing things that are too general to be useful
    too_general = [
        # "Atherosclerosis",
        "Neoplasm" "Tissues",
        "Malignant neoplastic disease",
        "Inflammation",
        "Wounds and Injuries",
        "Growth",
        #  'Brain',
        #  'Liver',
        "Apoptosis",
        "Infectious disease",
        "Cell Line",
        "Traumatic injury",
        "Lesion",
        #  'DNA',
        "Antibodies",
        "Mice",
        "Water",
        "Toxic effect",
        #  'ETIOL',
        #  'Kidney',
        #  'Glucose',
        "receptor",
        #  'Lung',
        "Oxidative Stress",
        #  'TNF gene',
        #  'Ethanol',
        "Cytokines",
        #  'Heart',
        #  'Obesity',
        "Surgical Procedures, Operative",
        #  'CD69 gene',
        #  'Neurons',
        "Cell Proliferation",
        #  'TNF protein, human',
        #  'Hypertensive disorder',
        "Reactive Oxygen Species",
        "Hypersensitivity",
        "Rats",
        "Obstruction",
        #  'Calcium',
        "Metabolism",
        #  'Nitric Oxide',
        "Inflammatory Response",
        #  'Proto-Oncogene Proteins c-akt',
        #  'Diabetes',
        #  'Blood',
        "Lipopolysaccharides",
        "Amino Acids",
        "Base Sequence",
        #  'Fibrosis',
        "Cessation of life",
        "Lipids",
        #  'Plasma',
        #  'Pain',
        "Cytoplasm",
        #  'Serum',
        "Child",
        #  'Interleukin-6',
        #  'Insulin',
        #  'Insulin Resistance',
        "Signal Transduction",
        #  'Stress',
        "tumor growth",
        #  'Breast cancer',
        "Organ",
        #  'NF-kappa B',
        #  'Interleukin-1beta',
        #  'Phosphotransferases',
        "Antioxidants",
        #  'Colorectal Neoplasms',
        "Ions",
        #  'RNA, Messenger',
        "Anti-Bacterial Agents",
        "Membrane",
        #  'AKT1 gene',
        "Cell Transformation, Neoplastic",
        #  'Genes, p53',
        #  'hepatocellular carcinoma',
        #  'Thrombosis',
        #  'Macrophages',
        #  'CD69 antigen',
        #  'IMPACT gene',
        #  'Fibroblasts',
        #  'Tissue Adhesions',
        #  'Phosphorylation',
        #  'ATP8A2 gene',
        "Transcription, Genetic",
        "Women",
        #  'COVID-19',
        #  'Hypoxia',
        "Immunity",
        "Antigens",
        "Muscles",
        "Transcription Factors",
        "Homologous Gene",
        #  "Alzheimer's disease",
        "cytotoxicity",
        #  'ATP8A2 protein, human',
        #  'Aging',
        "Binding Sites",
        "Cell Death",
        "MicroRNAs",
    ]

    nodes = nodes.filter(pl.col("name").is_in(too_general).not_())
    logger.info(
        f"... {nodes.shape[0]:,} Nodes remain after filtering out nodes that are too general"
    )
    prior_to_filtering = filt_edges.shape[0]

    filt_edges = filt_edges.filter(
        pl.col("h_id").is_in(nodes["id"]), pl.col("t_id").is_in(nodes["id"])
    )

    logger.info(
        f"... Number of edges prior to filtering out nodes that are too general: {prior_to_filtering:,}"
    )
    logger.info(
        f"... Number of edges after filtering out nodes that are too general: {filt_edges.shape[0]:,}"
    )
    counts = filt_edges["n_pmids"].value_counts().sort("count", descending=True)
    x = counts.to_pandas()
    for i in range(1, 11):
        logger.info(
            "... Edges with at least {} unique PMIDs: {:,}".format(
                i, (x.query("n_pmids >= @i")["count"].sum())
            )
        )

    logger.info("... Saving data")
    filt_edges.write_parquet(
        f"../data/edges_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )
    nodes.write_parquet(
        f"../data/nodes_{args.semmed_version}_consolidated_condensed_filtered_001.parquet"
    )

    logger.info("Complete processing 04_filter_low_abundance_edges.py\n")


if __name__ == "__main__":
    main(parse_args())
