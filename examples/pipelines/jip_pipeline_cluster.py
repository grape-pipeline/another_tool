#!/usr/bin/env python

import logging
import sys

# load the tools from a dedicated module
from demo_tools import *
from jip.cluster import SunGrid

def print_info():
    print "\n\n[The interleaver is finished]\n\n"

# add a listener to an existing tool
Interleaver.on_finish.append(print_info)

if __name__ == "__main__":
    # add logging support
    logging.basicConfig(level=logging.INFO,
                        format='[%(levelname)s] : %(message)s')

    pipeline = Pipeline()

    # add tools to the pipeline
    interleaver = pipeline.add(Interleaver())
    aligner = pipeline.add(Aligner())
    quantifier = pipeline.add(Quantifier())

    # configure the tools
    interleaver.reads = "../data/reads_1.fastq ../data/reads_2.fastq"
    interleaver.output = "interleaved.fastq"

    aligner.reads = interleaver.output
    aligner.index = "../data/genome.gem"
    aligner.result = "alignment.map"

    quantifier.alignment = aligner.result
    quantifier.annotation = "../data/genome.gtf"
    quantifier.output = "counts.txt"

    # catch validation errors and print them nicer
    try:
        pipeline.validate()
    except PipelineException, e:
        logging.error("%s" % (str(e)))
        sys.exit(1)

    # submit the pipeline
    grid = SunGrid()
    features = pipeline.submit(grid)

    for feature in features:
        print "Submitted job ", feature.jobid
