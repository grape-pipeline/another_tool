#!/usr/bin/env python
# This is a non-functional "quantifier" that
# mimic something like the flux capacitor

import argparse
import logging
import sys
import os


def _parse_alignment_line(line):
    """Parse linse like
    READ	ACGCGT CGCTAG	DDDDDD DDDDDD	chr1:+:6::chr1:-:20,chr2:+:100:6::chr2:-:120:6

    Returns a list of chromosome names
    """
    # get the alignment (4th element)
    alignments = line.strip().split("\t")[3]
    # split multiple alignments
    single_alignments = alignments.split(",")

    chromosomes = []
    for alignment in single_alignments:
        # split by :: to split paired alignments
        pairs = alignment.split("::")
        for pair in pairs:
            # add the chromosme name, which is the first element
            chromosomes.append(pair.split(":")[0])
    return chromosomes


def test_parse_alignments():
    line ="READ\tACGCGT CGCTAG\tDDDDDD DDDDDD\tchr1:+:6::chr1:-:20,chr2:+:100:6::chr2:-:120:6"
    assert _parse_alignment_line(line) == ["chr1", "chr1", "chr2", "chr2"]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] : %(message)s')

    parser = argparse.ArgumentParser(prog="quantifier", description="""
    Quantify a set of alignments
    """)
    parser.add_argument("-i", "--input", help="Input file")
    parser.add_argument("-o", "--output", help="Output file")
    parser.add_argument("--annotation", help="The genome annotation", required=True)
    parser.add_argument("--fail", help="Trigger a failure", action="store_true", default=False)
    parser.add_argument("--log", help="Set the log level",
                        choices=["DEBUG", "INFO", "WARN", "ERROR"],
                        default="ERROR")

    args = parser.parse_args()
    # set log level
    logging.getLogger().setLevel(
        {
            "ERROR": logging.ERROR,
            "WARN": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG
        }[args.log]
    )

    # do some checks
    if args.fail:
        logging.error("Fail requested")
        sys.exit(1)

    if not os.path.exists(args.annotation):
        logging.error("File not found : %s" % args.annotation)
        sys.exit(1)

    logging.info("Starting the quantification")


    in_file = sys.stdin
    if args.input is not None:
        in_file = open(args.input, "r")

    counts = {}
    for line in in_file:
        chrs = _parse_alignment_line(line)
        for chr in chrs:
            counts[chr] = counts.get(chr, 0) + 1

    if args.input is not None:
        in_file.close()

    f = sys.stdout
    if args.output is not None:
        f = open(args.output, "w")

    for name, count in counts.items():
        f.write("%s\t%d\n" % (name, count))

    if args.output is not None:
        f.close()

    logging.info("Done :)")
