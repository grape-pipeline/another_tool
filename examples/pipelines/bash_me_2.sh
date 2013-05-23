#!/bin/bash

ALIGNER=../tools/aligner
INTERLEAVE=../tools/interleave
QUANTIFIER=../tools/quantifier

# input data
INDEX=../data/genome.gem
ANNOTATION=../data/genome.gtf
READS="../data/reads_1.fastq ../data/reads_2.fastq"

$INTERLEAVE $READS | $ALIGNER --index $INDEX | $QUANTIFIER --annotation $ANNOTATION > counts.txt
