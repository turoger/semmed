import argparse
import gzip  # 2c
import json  # 2d
import os  # 2abc
import pickle  # 2abc
import sys  # 2a
import tarfile  # 2b
import urllib.request  # 2ab
import xml.etree.ElementTree as ET  # 2bc
from collections import defaultdict as dd  # 2c

import polars as pl  # 2ab
import requests  # 2d
from lxml import etree  # 2b
from tqdm import tqdm  # 2b

sys.path.append("../tools/")
from parallel import parallel_process


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Initial data cleaning of SemMedDB.
        Run this script to remove most errors prior to hetnet processing""",
        usage="02_id_to_publication_year.py [<args>] [-h | --help]",
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
    #
    # 02a-id_to_publication_year-PMC starts here
    #
    print("Running 02_id_to_publication_year.py")
    print("Map PMIDs to Year using PubMedCentral (approx. 1 minute)")
    print("... Downloading PubMed ID to Year Map from PubMed Central.")
    filename = "../data/PMC-ids-csv.gz"
    if not os.path.exists(filename):
        urllib.request.urlretrieve(
            "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/PMC-ids.csv.gz",
            filename=filename,
        )

    print(f"... Load and Retrieve PMIDs from pmid_list_{args.semmed_version}.txt")
    pmids = []
    with open(f"../data/pmid_list_{args.semmed_version}.txt", "r") as fin:
        for line in fin.readlines():
            pmids.append(line.strip())
    print(f"... ... Number of Retrieved PMIDs: {len(pmids):,}")
    print(f"... Load downloaded PubMed Central retrieved IDs, drop Null Values")

    with gzip.open(filename) as f:
        df = pl.read_csv(f.read(), ignore_errors=True)

    df = df.drop_nulls("PMID").with_columns(pl.col("PMID").cast(str))
    print(f"... ... Length of the dataframe: {len(df):,}")
    print(f'... ... Number of PMIDs in dataframe: {df["PMID"].unique().len():,}')

    print("... Converting dataframe to dictionary")
    pmid_to_year = dict(zip(df["PMID"].to_list(), df["Year"].to_list()))

    mapped = set(pmid_to_year.keys())
    no_map = set(pmids) - mapped
    print(f"... Number of PMIDs mapped: {len(no_map):,}")
    print(f"... Remaining PMIDs unmapped: {len(no_map):,}")
    print("... Exporting mapped pmid_to_year to ../data/pmid_to_year_PMC.pkl")
    pickle.dump(pmid_to_year, open("../data/pmid_to_year_PMC.pkl", "wb"))
    print("... Exporting unmapped pmid_to_year to ../data/no_map_PMC.pkl")
    pickle.dump(no_map, open("../data/no_map_PMC.pkl", "wb"))

    print("Complete.\n")

    #
    # 02B-id_to_publication_year-EuroPMC starts here
    #
    print("Map PMIDs to Year using Europe PMC")

    print("... Checking for PMC Lite Metadata tgz file")
    filename = "../data/PMCLiteMetadata.tgz"
    if not os.path.exists(filename):
        print(
            "... Downloading PubMed ID to Year Map from Europe PMC. (approx. 20 minutes)"
        )

        urllib.request.urlretrieve(
            "http://europepmc.org/ftp/pmclitemetadata/PMCLiteMetadata.tgz",
            filename=filename,
        )
    print("... Extract xml from the downloaded tgz. (approx. 3 minutes)")

    if not os.path.exists("../data/out"):
        pmc_tar = tarfile.open("../data/PMCLiteMetadata.tgz")
        pmc_tar.extractall("../data", filter="fully_trusted")

    class XML2DataFrame:

        def __init__(self, xml_file):
            self.root = ET.parse(
                xml_file, parser=etree.XMLParser(recover=True, encoding="utf-8")
            ).getroot()

        def parse_root(self, root):
            """Return a list of dictionaries from the text
            and attributes of the children under this XML root."""
            return [self.parse_element(child) for child in root.getchildren()]

        def parse_element(self, element):
            """Collect {key:attribute} and {tag:text} from thie XML
            element and all its children into a single dictionary of strings."""
            parsed = {c.tag: c.text for c in element.getchildren()}
            return parsed

        def process_data(self):
            """Initiate the root XML, parse it, and return a dataframe"""
            structure_data = self.parse_root(self.root)
            return pl.DataFrame(
                structure_data,
                schema={
                    "id": pl.Utf8,
                    "source": pl.Utf8,
                    "pmid": pl.Utf8,
                    "pmcid": pl.Utf8,
                    "DOI": pl.Utf8,
                    "title": pl.Utf8,
                    "AuthorList": pl.Utf8,
                    "JournalTitle": pl.Utf8,
                    "Issue": pl.Utf8,
                    "JournalVolume": pl.Utf8,
                    "PubYear": pl.Utf8,
                    "JournalIssn": pl.Utf8,
                    "PageInfo": pl.Utf8,
                    "PubType": pl.Utf8,
                    "IsOpenAccess": pl.Utf8,
                    "InEPMC": pl.Utf8,
                    "InPMC": pl.Utf8,
                    "HasPDF": pl.Utf8,
                    "HasBook": pl.Utf8,
                    "HasSuppl": pl.Utf8,
                    "CitedByCount": pl.Utf8,
                    "HasReferences": pl.Utf8,
                    "HasLabsLinks": pl.Utf8,
                    "FirstIndexDate": pl.Utf8,
                    "FirstPublicationDate": pl.Utf8,
                    "PublicationStatus": pl.Utf8,
                },
            )

    print("... Processing XML files into a dataframe.")
    frames = []
    base = (
        "../data/out"  # this is where the output of the Europe PMC object was extracted
    )
    files = sorted(
        [f for f in os.listdir(base) if f.endswith(".xml")],
        key=lambda x: int(x.split(".")[1]),
    )

    for file in tqdm(files):
        xml2df = XML2DataFrame(os.path.join(base, file))
        xml_dataframe = xml2df.process_data()
        frames.append(xml_dataframe)

    result = pl.concat(frames)
    pmid_mapper = dict(zip(result["pmid"].to_list(), result["PubYear"].to_list()))
    print(f"... Size of dataframe extracted from Europe PMC: {len(result):,}")
    print(f"... Importing unmapped PMIDs (missing dates)")
    # prev_no_map = pickle.load(open("../data/no_map_PMC.pkl", "rb"))
    print(f"... Number of unmapped PMIDs: {len(no_map):,}")
    print("... Mapping PMIDs using Europe PMC")
    mapped = set(pmid_mapper.keys())
    new_no_map = no_map - mapped

    print(f"... Number of remaining unmapped PMIDs: {len(new_no_map):,}")
    print(f"... Exporting mapped pmid_to_year to ../data/pmid_to_year_Eur.pkl")
    pickle.dump(pmid_mapper, open("../data/pmid_to_year_Eur.pkl", "wb"))

    print(f"... Exporting unmapped pmid_to_year to ../data/no_map_Eur.pkl")
    pickle.dump(new_no_map, open("../data/no_map_Eur.pkl", "wb"))

    print("Complete. \n")

    #
    # 02C-id_to_publication_year-NLM_Baseline starts here
    #

    print(
        "Map PMIDs to Year using National Library of Medicine Baseline documents (approx. 10 minutes)"
    )
    print(f"... processing xmls")
    base = "../data/baseline/"
    files = [f for f in os.listdir(base) if f.endswith(".xml.gz")]

    # Last 4 characters before .xml indicate file's order
    files = sorted(files, key=lambda f: int(f.split(".")[0][-4:]))
    print(f"... Number of files to process: {len(files):,}")
    print(f"... Processing")

    results = parallel_process(files, get_id_to_year_map, n_jobs=32, front_num=0)
    print(f"... Files processed: {len(results):,}")

    # print(f'... Add a year to every entry in the file')
    adict = dd(int)
    for r in results:
        adict[type(r)] += 1
    id_to_year = {}
    print(f"... Filter out malformed xml entries from the year dictionary")
    for r in results:
        try:
            id_to_year.update(r)
        except:
            print("... ... Did not update entry in the dictionary: ", r)
            pass

    print(f"... Number of entries in updated year dict: {len(id_to_year):,}")
    id_to_year_filt = {k: v for k, v, in id_to_year.items() if v is not None}
    print(
        f"... Number of entries in updated year dict not NaN: {len(id_to_year_filt):,}"
    )
    print(f"... Load past unmapped PMIDs from ../data/no_map_Eur.pkl")
    # prev_no_map = pickle.load(open("../data/no_map_Eur.pkl", "rb"))
    prev_no_map = new_no_map
    still_no_map = set(prev_no_map) - set(id_to_year.keys())
    print(f"... Remaining unmapped entries: {len(still_no_map):,}")

    print(f"... Exporting mapped pmid_to_year to ../data/pmid_to_year_NLM.pkl")
    pickle.dump(id_to_year, open("../data/pmid_to_year_NLM.pkl", "wb"))
    print(f"... Exporting unmapped pmid_to_year to ../data/no_map_NLM.pkl")
    pickle.dump(still_no_map, open("../data/no_map_NLM.pkl", "wb"))
    print(
        "Complete. 02C-id_to_publication_year-NLM_Baseline.py has finished running. \n"
    )

    #
    # 2D-id_to_publication_year-ebi_API starts here
    #
    # still_no_map = pickle.load(open("../data/no_map_NLM.pkl", "rb"))

    URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=ext_id:{}%20src:med&format=json"

    new_map = {}

    def get_date(result):
        """Gets date from queried json file. If no items retrieved, returns none, otherwise looks for the oldest date"""
        res = result["resultList"]["result"]
        if len(res) == 1 and type(res[0]) == dict:
            """Gets date if result is 1"""
            return res[0].get("firstPublicationDate", None)

        elif len(res) > 1 and type(res[0]) == dict:
            """Gets oldest date if results greater than 1"""
            dates = []
            for r in res:
                date = res[0].get("firstPublicationDate", "9999-99-99")
                dates.append(date)
            return min(dates)
        return None

    print("Retrieving PMID dates from Europe PMC API")
    while len(new_map) <= len(still_no_map):
        # loads pickle file so you don't start from scratch
        new_map_file = os.path.join("../data", "new_map.pkl")
        if os.path.exists(new_map_file):
            with open(new_map_file, "rb") as f:
                new_map = pickle.load(f)
            print(
                f"... importing already requested pmids. {len(new_map):,}/{len(still_no_map):,} already requested."
            )
        else:
            print(
                f"... no exported file found, starting from scratch. This can take over 2 days to run."
            )
            new_map = {}
        # extract out the remaining unmapped items
        still_no_map = still_no_map.difference(set(new_map.keys()))
        print(f"... remaining unmapped: {len(still_no_map):,}")

        for pmid in tqdm(list(still_no_map)):
            r = requests.get(URL.format(pmid))
            # prevents broken json decode from erroring out
            try:
                result = json.loads(r.text)
                date = get_date(result)
                new_map[pmid] = date
            except:
                new_map[pmid] = None

            # save every 500 so you don't lose progress in case of disconnect.
            if (len(new_map) + 1) % 500 == 0:
                with open(new_map_file, "wb") as f:
                    pickle.dump(new_map, f)

    # final save
    with open(new_map_file, "wb") as f:
        pickle.dump(new_map, f)

    new_map = {k: v for k, v in new_map.items() if v is not None}
    print(f"... Mapped PMID entry to a date: {len(new_map):,}")

    final_no_map = still_no_map - set(new_map.keys())
    print(f"... Remaining unmapped entries: {len(final_no_map):,}")

    print("\nExporting files")
    print(f"... Exporting mapped pmid_to_year to ../data/pmid_to_year_EBI.pkl")
    pickle.dump(new_map, open("../data/pmid_to_year_EBI.pkl", "wb"))
    print("... Exporting unmapped pmid_to_year to ../data/no_map_EBI.pkl")
    pickle.dump(final_no_map, open("../data/no_map_EBI.pkl", "wb"))
    print("Complete. 02D-id_to_publication_year-ebi_API.py has finished running. \n")


# 2c functions
def get_child_tag(child, tag):
    for c in child.getchildren():
        if c.tag == tag:
            return c


def get_year_from_article(article):
    journal = get_child_tag(article, "Journal")
    issue = get_child_tag(journal, "JournalIssue")
    pub_date = get_child_tag(issue, "PubDate")
    year = get_child_tag(pub_date, "Year")
    if year is not None:
        return year.text


def get_year_from_pubmed(pubmed_data):
    history = get_child_tag(pubmed_data, "History")
    for child in history.getchildren():
        if child.tag == "PubMedPubDate" and child.items()[0][1] == "pubmed":
            year = get_child_tag(child, "Year")
    if year is not None:
        return year.text


def get_pmid_year(pubmed_article):
    medline_cit = get_child_tag(pubmed_article, "MedlineCitation")
    pubmed_data = get_child_tag(pubmed_article, "PubmedData")
    pmid = get_child_tag(medline_cit, "PMID")
    try:
        year = get_year_from_pubmed(pubmed_data)
    except:
        article = get_child_tag(medline_cit, "Article")
        year = get_year_from_article(article)
    if pmid is not None:
        pmid = pmid.text
    return pmid, year


def get_id_to_year_map(file, base: str = "../data/baseline/"):
    id_to_year = {}
    tree = ET.parse(
        gzip.open(os.path.join(base, file)),
        parser=etree.XMLParser(recover=True, encoding="utf-8"),
    )
    root = tree.getroot()
    for cit in root.getchildren():
        pmid, year = get_pmid_year(cit)
        id_to_year[pmid] = year
    return id_to_year


if __name__ == "__main__":
    main(parse_args())
