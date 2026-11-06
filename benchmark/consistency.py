"""Compare ordered log records selected by a caller-provided regular expression."""

import argparse
import re


def extract_records(log_file, pattern):
    expression = re.compile(pattern)
    records = []
    with open(log_file, "r", encoding="utf-8") as source:
        for line in source:
            records.extend(expression.findall(line))
    return records


def compare_sequences(log_files, pattern, allow_prefix=False):
    if not log_files:
        raise ValueError("At least one log file is required")
    sequences = {filename: extract_records(filename, pattern) for filename in log_files}
    for filename, records in sequences.items():
        if not records:
            print(f"No matching records in {filename}")
            return False
    reference, expected = next(iter(sequences.items()))
    consistent = True
    for filename, records in sequences.items():
        if filename == reference:
            continue
        for index, (left, right) in enumerate(zip(expected, records)):
            if left != right:
                print(f"Record {index}: {reference}: {left!r}; {filename}: {right!r}")
                consistent = False
        if not allow_prefix and len(expected) != len(records):
            print(
                f"Record count differs: {reference}: {len(expected)}; {filename}: {len(records)}"
            )
            consistent = False
    if consistent:
        print(
            "Selected sequences have a consistent prefix."
            if allow_prefix
            else "All selected record sequences match."
        )
    return consistent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pattern",
        required=True,
        help="Regular expression selecting comparable records",
    )
    parser.add_argument(
        "--allow-prefix",
        action="store_true",
        help="Allow different sequence lengths at shutdown",
    )
    parser.add_argument("log_files", nargs="+")
    args = parser.parse_args()
    raise SystemExit(
        0
        if compare_sequences(
            args.log_files, args.pattern, allow_prefix=args.allow_prefix
        )
        else 1
    )
