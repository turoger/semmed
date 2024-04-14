import logging
import pathlib
import subprocess
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

import click
import git
from docdata import (  # functions written by cthoyt to convert docstring to a dictionary
    get_docdata,
    parse_docdata,
)
from more_click import verbose_option
from pykeen.datasets import PathDataset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)


__all__ = ["TimeResolvedKG"]


@parse_docdata
class TimeResolvedKG(PathDataset):
    """
    A Time-Resolved Knowledge Graph dataset based on the SemMedDB dataset. The purpose of this datasource is to facillitate easier access to the SemMedDB dataset which cannot be distributed without a UMLS license. The dataset is built using the `build.sh` script at github.com/turoger/semmed and streamlines the work by Mayers et. al (2019) utilizing the polars library. Initialization of this dataset is still slow, since a lot of resources are downloaded and processed; but its much faster than the original method or executing jupyter notebooks.

    There are a total of 70+ time seperated datasets in this object. Each dataset is split by time, where the test/valid sets comprise of approved drug disease indications from DrugCentral. There are two ways to split each dataset: one is by train/test and another is by train/test/valid. In the former, triples are allocated to train if they are facts prior to or on a specified year; triples are allocated to test if they are facts after a specified  year. In the latter, triples are allocate to train and test if they are facts prior to or on a specified year; triples are allocated to valid if they are facts after a specified year.

    This time-resolved dataset consists of 6 metanodes (types): Chemicals & Drugs, Disorders,Genes & Molecular Sequences, Anatomy, Physiology, and Phenomena. The number of relations will vary based on the selected parameters below.

    In order to use this pipeline, you *must* have a UMLS API key. You can obtain one by registering at https://uts.nlm.nih.gov/home.html. Additionally, you must have access to a PostgreSQL database to process some parts of the data. The dataset is built using the following required arguments supplied to `build_dataset_kwargs`:
    - apikey        from a UMLS Metathesaurus Account
    - host, user, pass, port (postgresql host/user/password/port).

    Other parameters supplied to `build_dataset_kwargs` are optional:
    - umls_date             version of UMLS, ex: "2023AA"
    - semmed_ver            version of SemMedDB, ex: "VER43_R"
    - dc_date               DrugCentral dataset version, ex: "20220822" or "11012023"
    - include_time          whether to include time in the output dataset, {0,1}
    - split_ttv             whether to splt dataset into train/test/valid instead of default train/test, {0,1}
    - split_hpo             whether to create a hyperparameter optimization set for a specific year, {0,1}
    - hpo_year              the year to do hyperparameter optimization on, ex: "1987"
    - drop_negative_edges   whether to drop negative edges from the dataset, {0,1}
    - convert_neg           whether to turn negative edges into neutral edges {0,1}
    - include_direction     whether to include directional edges in the dataset, {0,1}
    """

    def __init__(
        self,
        cache_root: Optional[str] = None,
        build_dataset_kwargs=Optional[Mapping[str, Any]],
        year: Optional[str] = None,
    ):
        self.cache_root = self._help_cache(cache_root)
        self.url = "git@github.com:turoger/semmed"

        # parse dataset arguments
        self.dataset_kwargs = build_dataset_kwargs
        self.base_dir = self.dataset_kwargs.get(
            "base_dir", "../data/time_networks-6_metanode"
        )
        self.year = year
        self.include_time = (
            "_time" if self.dataset_kwargs.get("include_time", 0) else "_notime"
        )
        self.train_test_valid = (
            "_ttv" if self.dataset_kwargs.get("split_ttv", 0) else ""
        )

        # get relative paths after parsing dataset_kwargs
        ds_file_str = f"{self.train_test_valid}{self.include_time}"
        self._relative_path = pathlib.PurePath(
            self.cache_root, self.base_dir.replace("../", ""), self.year
        )
        self._relative_training_path = pathlib.PurePath(
            self._relative_path, f"train{ds_file_str}.txt"
        )
        self._relative_testing_path = pathlib.PurePath(
            self._relative_path, f"test{ds_file_str}.txt"
        )
        self._relative_validation_path = pathlib.PurePath(
            self._relative_path, f"valid{ds_file_str}.txt"
        )

        logger.info(f"relative path: {self._relative_path}")

        training_path = self._get_paths(self._relative_training_path)
        testing_path = self._get_paths(self._relative_testing_path)
        validation_path = self._get_paths(self._relative_validation_path)
        logger.info(f"training path: {training_path}")
        logger.info(f"testing path: {testing_path}")

        # if validation doesn't exist, don't pass validation to super
        if self.train_test_valid != "_ttv":
            validation_path = None
            self._check_paths = [training_path, testing_path]
        else:
            logger.info(f"validation path: {validation_path}")
            self._check_paths = [training_path, testing_path, validation_path]

        super().__init__(
            training_path=training_path,
            testing_path=testing_path,
            validation_path=validation_path,
            eager=True,
            create_inverse_triples=True,
        )

    def _get_paths(self, relative_file_path) -> pathlib.Path:
        """The paths where the extracted files can be found."""
        return self.cache_root.joinpath(relative_file_path)

    def _extract(self, git_file: pathlib.Path) -> None:
        """Extract from the downloaded file."""

        # import github repository into the default pykeen directory
        # build the dataset call
        to_call = ["bash", "build.sh"]
        # remove any flags that are 0
        self.dataset_kwargs = {k: v for k, v in self.dataset_kwargs.items() if v != 0}

        [  # adds '--' to keys and appends both keys and  values to 'to_call'
            to_call.append(f"--{v}") if i == 0 else to_call.append(v)
            for item in self.dataset_kwargs.items()
            for i, v in enumerate(item)
        ]

        to_call = [
            i for i in to_call if i != 1
        ]  # removes any '1' value from the call string

        logger.info(f"Extracting dataset with {to_call} from {git_file}.")
        subprocess.run(to_call, cwd=git_file)

    def _get_git(self) -> pathlib.Path:
        """
        Function to get the git file and store in the cache root ./data/pykeen/datasets/timeresolvedkg
        """
        logger.info(f"Requesting dataset from {self.url}")
        if not self.cache_root.exists():
            logger.info(f"Cloning repository from {self.url} to {self.cache_root}.")
            git.Repo.clone_from(url=self.url, to_path=self.cache_root)
            logger.info(f"Dowloaded repository to {self.cache_root}.")
        else:
            logger.info(
                f"Repository already exists at {self.cache_root}. Skipping download."
            )

    # docstr-coverage: inherited
    def _load(self) -> None:  # noqa: D102
        all_unpacked = all(path.is_file() for path in self._check_paths)
        logger.info(f"Checking if all files are unpacked: {all_unpacked}.")

        if not all_unpacked:
            logger.info(f"Download and extract the dataset from {self.url}.")
            git_file = self._get_git()
            self._extract(git_file=self.cache_root)
            logger.info(f"Extracted to {self.cache_root}.")

        logger.info(f"Loading dataset from {self._relative_path}.")

        super()._load()


@click.command()
@verbose_option
def _main():
    from pykeen.datasets import get_dataset

    ds = get_dataset(dataset="TimeResolvedKG")
    ds.summarize()


if __name__ == "__main__":
    _main()
