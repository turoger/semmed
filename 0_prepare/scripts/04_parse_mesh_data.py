import argparse
import os
import pickle
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict

from lxml import etree

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
        usage="04_parse_mesh_data.py [<args>] [-h | --help]",
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
    logger.info("Running 04_parse_mesh_data.py")
    logger.info("... Downloading mesh files from NIH via ftp")
    date = args.umls_date[0:4]

    supp_filename = f"../data/supp{date}.xml"
    desc_filename = f"../data/desc{date}.xml"

    if not os.path.exists(supp_filename):
        logger.info(f"... Downloading supp{date}.xml")
        urllib.request.urlretrieve(
            f"https://nlmpubs.nlm.nih.gov/projects/mesh/{2023}/xmlmesh/supp{date}.xml",
            filename=supp_filename,
        )
    if not os.path.exists(desc_filename):
        logger.info(f"... Downloading desc{date}.xml")
        urllib.request.urlretrieve(
            f"https://nlmpubs.nlm.nih.gov/projects/mesh/{2023}/xmlmesh/desc{date}.xml",
            filename=desc_filename,
        )

    logger.info(f"... Get file roots")
    desc_root = ET.parse(
        desc_filename, parser=etree.XMLParser(recover=True, encoding="utf-8")
    ).getroot()
    supp_root = ET.parse(
        supp_filename, parser=etree.XMLParser(recover=True, encoding="utf-8")
    ).getroot()
    # get child tag for Descriptor UI

    def extract_to_dict(root, a_dict: dict, a_key: str, a_val: str) -> dict:
        """
        parses xml for tags, and assigns the tag to 'a_key' or to 'a_val'
        """

        for item in root.getchildren():
            for child in item.getchildren():
                if child.tag == a_key:
                    uid = child.text
                if child.tag == a_val:
                    name = child.getchildren()[0].text
            a_dict[uid] = name

        return a_dict

    id_to_name = dict()
    id_to_name = extract_to_dict(
        desc_root, id_to_name, "DescriptorUI", "DescriptorName"
    )

    id_to_name = extract_to_dict(
        supp_root, id_to_name, "SupplementalRecordUI", "SupplementalRecordName"
    )

    logger.info(f"... {len(id_to_name):,} MeSH Descriptor/Supplemental IDs extracted")
    filename = "../data/MeSH_DescUID_to_Name.pkl"
    logger.info(f"... exporting to {filename}")
    with open(filename, "wb") as f:
        pickle.dump(id_to_name, f)

    logger.info(f"... Getting tree-values for children")
    id_to_treenumbs = defaultdict(list)
    for descriptor in desc_root.getchildren():
        for child in descriptor.getchildren():
            if child.tag == "DescriptorUI":
                uid = child.text
            if child.tag == "TreeNumberList":
                for tn in child.getchildren():
                    id_to_treenumbs[uid].append(tn.text)

    filename = "../data/MeSH_DescUID_to_TreeNumbs.pkl"
    logger.info(f"... exporting to {filename}")
    with open(filename, "wb") as f:
        pickle.dump(id_to_treenumbs, f)

    logger.info("Complete. 04_parse_mesh_data.py has finished running. \n")


if __name__ == "__main__":
    main(parse_args())
