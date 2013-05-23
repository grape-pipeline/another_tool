#!/bin/bash

# enable pipe fails
set -o pipefail

ALIGNER=../tools/aligner
INTERLEAVE=../tools/interleave
QUANTIFIER=../tools/quantifier

# input data
INDEX=../data/genome.gem
ANNOTATION=../data/genome.gtf
READS="../data/reads_1.fastq ../data/reads_2.fastq"

$INTERLEAVE $READS | $ALIGNER --index $INDEX --fail | $QUANTIFIER --annotation $ANNOTATION > counts.txt
