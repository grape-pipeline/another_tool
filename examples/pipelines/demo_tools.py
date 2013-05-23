from another.tools import BashTool, ValidationException
from another.pipelines import Pipeline, PipelineException

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

    def validate(self, args, incoming=None):
        BashTool.validate(self, args, incoming)
        # check that this is a gem index
        if not args["index"].endswith(".gem"):
            raise ValidationException({"index": "The given index does not seem to be a gem index!"})


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
