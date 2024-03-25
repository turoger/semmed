import argparse
import subprocess


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description=r"""Initial processing SemMedDB.
        This script is a wrapper on the initial processing script for SemMedDB. The wrapper's purpose is to run all initial processing scripts together, so that `conda run` is only initialized once. This helps with the scripts run-time overhead
        """,
        usage="preprocessing.py [<args>] [-h | --help]",
    )

    parser.add_argument(
        "-v",
        "--semmed_version",
        default="VER43_R",
        type=str,
        help="version number, a string, for SemMed dump",
    )
    parser.add_argument(
        "-D",
        "--umls_date",
        default="2023AA",
        type=str,
        help="downloaded semmed version year followed by two capitalized, alphabetical characters",
    )

    return parser.parse_args(args)


def main(args):
    subprocess.run(
        [
            "python",
            "./scripts/01_initial_data_clean.py",
            "--semmed_version",
            args.semmed_version,
        ]
    )
    subprocess.run(
        [
            "python",
            "./scripts/02_id_to_publication_year.py",
            "--semmed_version",
            args.semmed_version,
        ]
    )
    subprocess.run(
        [
            "python",
            "./scripts/03_umls_cui_to_mesh_descriptorID.py",
            "--semmed_version",
            args.semmed_version,
            "--umls_date",
            args.umls_date,
        ]
    )
    subprocess.run(
        ["python", "./scripts/04_parse_mesh_data.py", "--umls_date", args.umls_date]
    )
    subprocess.run(
        [
            "python",
            "./scripts/05_mesh_id_to_name_via_umls.py",
        ]
    )


if __name__ == "__main__":
    main(parse_args())
