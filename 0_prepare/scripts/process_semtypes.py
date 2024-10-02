import argparse

parser = argparse.ArgumentParser(description="Process SemTypes file from UMLS")
parser.add_argument("-i", "--input_semtype_file", type=str, required=True)
parser.add_argument(
    "-o", "--out_dir", type=str, required=True, default="../../data/SemTypes.txt"
)

args = parser.parse_args()


def extract_semtypes(input_file: str):
    """
    :input_file: Takes a string pointing to the file containing semantic types from UMLS and extracts the semantic types, their abbreviations, and their unique identifiers in free text format
    :return: a list of tuples containing the abbreviation, unique identifier, and semantic type
    """
    with open(input_file, "r") as f:
        tuple_ls = []

        for line in f:
            if line.startswith("UI"):
                ui = line.strip("\n").split(": ")[-1]
            if line.startswith("STY"):
                sty = line.strip("\n").split(": ")[-1]
            if line.startswith("ABR"):
                abr = line.strip("\n").split(": ")[-1]
                tuple_ls.append((abr, ui, sty))

    return tuple_ls


def write_semtypes(output_file: str, semtypes_ls: list):
    """
    :output_file: Takes a string pointing to the file to write the semantic types, their abbreviations, and their unique identifiers in free text format
    :semtypes_ls: Takes a list of tuples containing the abbreviation, unique identifier, and semantic type
    :return: None
    """
    with open(output_file, "w") as f:
        for tup in semtypes_ls:
            f.write(f"{tup[0]}|{tup[1]}|{tup[2]}\n")


def main(args):
    semtypes_ls = extract_semtypes(args.input_semtype_file)
    write_semtypes(args.out_dir, semtypes_ls)


if __name__ == "__main__":
    main(args)
