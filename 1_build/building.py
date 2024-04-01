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
        "-i",
        "--include_direction",
        default=False,
        action="store_true",
        help='Includes or removes direction component of metapath. i.e "CDpo>G" -> "CDpoG", "Cct>C" -> "CctC". Directionality cannot be applied if "convert_negative_relations" is False or "drop_negative_relations" is False',
    )
    parser.add_argument(
        "-d",
        "--dc_date",
        default="11012023",
        type=str,
        help="version date, a string, for DrugCentral dump",
    )
    parser.add_argument(
        "-b",
        "--base_dir",
        default="../data/time_networks-6_metanode",
        type=str,
        help="directory to build the time-based dataset",
    )
    parser.add_argument(
        "-o",
        "--split_hyperparameter_optimization",
        default=False,
        action="store_true",
        help="create a hyperparameter optimization split, where the train triples are equal to or less than the given year, and test/valid triples are greater than the given year",
    )

    parser.add_argument(
        "-t",
        "--split_train_test_valid",
        default=False,
        action="store_true",
        help="split data into train/test/valid sets, where train/test triples are equal to or less than the given year, and valid triples are greater than the given year. If 'False', only split into train and test, where train triples are equal to or less than the given year, and test triples are greater than the given year.",
    )
    parser.add_argument(
        "-x",
        "--include_time",
        default=False,
        action="store_true",
        help="include time in the dataset",
    )
    parser.add_argument(
        "-y",
        "--hpo_year",
        default="1987",
        type=str,
        help="Year to split the hyperparameter optimization dataset. Default is 1987. Only used if split_hyperparameter_optimization is True.",
    )

    return parser.parse_args(args)


def main(args):
    # script 1 options
    script_1_dict = {
        "drop_negative_relations": args.drop_negative_relations,
        "convert_negative_relations": args.convert_negative_relations,
        "include_direction": args.include_direction,
    }
    script_1_ls = [
        "python",
        "./scripts/01_build_hetnet_polars.py",
        "--semmed_version",
        args.semmed_version,
    ]
    for k, v in script_1_dict.items():
        if v == 1:
            script_1_ls.append(f"--{k}")

    subprocess.run(script_1_ls)

    # script 2
    subprocess.run(
        [
            "python",
            "./scripts/02_Merge_Nodes_via_ID_xrefs_polars.py",
            "--semmed_version",
            args.semmed_version,
            "--dc_date",
            args.dc_date,
        ]
    )

    # script 3
    # script 3 options
    script_3_dict = {
        "drop_negative_relations": args.drop_negative_relations,
        "convert_negative_relations": args.convert_negative_relations,
    }
    script_3_ls = [
        "python",
        "./scripts/03_Condense_edge_semmantics_polars.py",
        "--semmed_version",
        args.semmed_version,
    ]
    for k, v in script_3_dict.items():
        if v == 1:
            script_3_ls.append(f"--{k}")
    subprocess.run(script_3_ls)

    # script 4
    subprocess.run(
        [
            "python",
            "./scripts/04_filter_low_abundance_edges_polars.py",
            "--semmed_version",
            args.semmed_version,
        ]
    )

    # script 5
    subprocess.run(
        [
            "python",
            "./scripts/05_Keep_Six_relevant_metanodes_polars.py",
            "--semmed_version",
            args.semmed_version,
        ]
    )

    # script 6
    subprocess.run(
        [
            "python",
            "./scripts/06_Resolve_Network_Edges_by_Time_polars.py",
            "--semmed_version",
            args.semmed_version,
            "--base_dir",
            args.base_dir,
        ]
    )

    # script 7
    script_7_dict = {
        "split_hyperparameter_optimization": args.split_hyperparameter_optimization,
        "split_train_test_valid": args.split_train_test_valid,
        "include_time": args.include_time,
    }
    script_7_ls = [
        "python",
        "./scripts/07_Build_data_split.py",
        "--semmed_version",
        args.semmed_version,
        "--base_dir",
        args.base_dir,
        "--hpo_year",
        args.hpo_year,
    ]
    for k, v in script_7_dict.items():
        if v == 1:
            script_7_ls.append(f"--{k}")

    subprocess.run(script_7_ls)


if __name__ == "__main__":
    main(parse_args())
