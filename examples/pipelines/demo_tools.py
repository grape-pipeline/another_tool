from jip.tools import Tool, ValidationException
from jip.pipelines import Pipeline, PipelineException

class Interleaver(Tool):
    inputs = {
        "reads": None,
    }
    outputs = {
        "output": "reads.fastq"
    }
    command = '''
        ../tools/interleave --log DEBUG -o ${output} ${reads}
    '''


class Aligner(Tool):
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

    def validate(self, args, incoming=None):
        Tool.validate(self, args, incoming)
        # check that this is a gem index
        if not args["index"].endswith(".gem"):
            raise ValidationException({"index": "The given index does not seem to be a gem index!"})


class Quantifier(Tool):
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
