#!/usr/bin/env python
"""Tests for the base tool class
and its implementation
"""

from jip.tools import Tool, ToolException


def test_tool_with_valid_tool_name():
    class MyTool(Tool):
        name = "MyTool"

        def call(self):
            pass
    mytool = MyTool()
    assert mytool is not None


def test_tool_with_invalid_tool_name():
    class MyTool(Tool):
        def call(self):
            pass
    t = MyTool()
    assert t is not None
    assert t.name == "MyTool", t.name


def test_tool_optional_variables():
    class MyTool(Tool):
        name = "MyTool"

        def call(self):
            pass
    MyTool()
    assert MyTool.long_description is None
    assert MyTool.short_description is None
    assert MyTool.version is None


def test_tool_optional_variables_version_set():
    class MyTool(Tool):
        name = "MyTool"
        version = "1.0"

        def call(self):
            pass

    MyTool()
    assert MyTool.long_description is None
    assert MyTool.short_description is None
    assert MyTool.version == "1.0"


def test_simple_python_tool_definition():
    class MyTool(Tool):
        name = "MyTool"

        def call(self, args):
            return args["a"] + args["b"]
    i = MyTool()
    assert i.call({"a":1, "b":2}) == 3


def test_command_rendering_with_args_and_kwargs():
    class MyTool(Tool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${a} + ${b} with addition=${addition}"
    t = MyTool()
    assert t.get_command(
        {"a": 1, "b": 2, "addition":"yeah"}) == "1 + 2 with addition=yeah"


def test_interpreter_tool_returns_method_none():
    class MyTool(Tool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${a} + ${b} with addition=${addition}"

    t = MyTool()
    assert t.returns(None) is None


def test_interpreter_tool_returns_method_string():
    class MyTool(Tool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${a} + ${b} with addition=${addition}"
        outputs = {"output": "testme"}

    t = MyTool()
    assert t.returns(None) == ["testme"]


def test_interpreter_tool_returns_method_function():
    class MyTool(Tool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${a} + ${b} with addition=${addition}"
        outputs = {"output": lambda t: "testme"}

    t = MyTool()
    assert t.returns(None) == ["testme"]


def test_on_start_listener_calls():
    called = [False]

    def my_start(tool, args):
        called[0] = True

    class MyTool(Tool):
        name = "Mytool"
        on_start = [my_start]

        def call(self, args):
            pass

    t = MyTool()
    t.run(None)
    assert called[0] is True


def test_paramter_defs_and_file_cleanup():
    class FastQC(Tool):
        name = "fastqc"
        command = """
        fastqc ${" ".join(args)}
        """

        def returns(self, *args, **kwargs):
            import re
            return ["%s_fastqc.zip" % (
                re.sub('(\.fastq|\.bam|\.sam|\.txt)+(\.gz|\.bz2)*$', '', x))
                for x in args]
    t = FastQC()

