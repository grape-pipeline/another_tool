#!/usr/bin/env python

from another.tools import BashTool
from another.pipelines import Pipeline


class Interleaver(BashTool):
    inputs = {
        "reads": None,
    }
    outputs = {
        "output": "reads.fastq"
    }
    command = '''
        ../tools/interleave --log DEBUG -o ${output} ${reads}
    '''


class Aligner(BashTool):
    inputs = {
        "index": None,
        "reads": None
    }
    outputs = {
        "result": None
    }
    command = """
    ../tools/aligner --log DEBUG --index ${index} -i ${reads} -o ${result}
    """


class Quantifier(BashTool):
    inputs = {
        "annotation": None,
        "alignment": None
    }
    outputs = {
        "output": None
    }
    command = """
    ../tools/quantifier --log DEBUG --annotation ${annotation} -i ${alignment} -o ${output}
    """


if __name__ == "__main__":
    pipeline = Pipeline()

    # add tools to the pipeline
    interleaver = pipeline.add(Interleaver())
    aligner = pipeline.add(Aligner())
    quantifier = pipeline.add(Quantifier())

    # configure the tools
    interleaver.reads = "../data/reads_1.fastq ../data/reads_2.fastq"
    interleaver.output = "reads.fastq"

    aligner.reads = interleaver.output
    aligner.index = "../data/genome.gem"
    aligner.result = "alignment.map"

    quantifier.alignment = aligner.result
    quantifier.annotation = "../data/genome.gtf"
    quantifier.output = "counts.txt"

    pipeline.validate()
    pipeline.run()
