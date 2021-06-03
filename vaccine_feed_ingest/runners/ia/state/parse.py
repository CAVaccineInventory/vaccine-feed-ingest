#!/usr/bin/env python3
import json
import pathlib
import sys


def zip_infile_outfile_pairs(input_dir, output_dir):
    infile_paths = input_dir.glob("*.json")
    for infile_path in infile_paths:
        filename = infile_path.stem
        outfile_path = output_dir.joinpath(f"{filename}.parsed.ndjson")
        yield infile_path, outfile_path


def main(argv):
    output_dir = pathlib.Path(argv[1])
    input_dir = pathlib.Path(argv[2])

    for infile_path, outfile_path in zip_infile_outfile_pairs(input_dir, output_dir):
        with open(infile_path, "r") as fin:
            contents = json.load(fin)
            # The raw file encodes each record as a string instead of a structured json, so we need a
            # second round of deserialization
            records = json.loads(contents["Result"])
        with open(outfile_path, "w") as fout:
            for record in records:
                json.dump(record, fout)
                fout.write("\n")


if __name__ == "__main__":
    main(sys.argv)
