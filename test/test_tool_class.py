#!/usr/bin/env python
"""Tests for the base tool class
and its implementation
"""

from another import Tool, InterpretedTool, ToolException


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

        def call(self, a, b):
            return a + b
    i = MyTool()
    assert i.call(1, 2) == 3
    assert i(1, 2) == 3


def test_command_rendering_with_args_and_kwargs():
    class MyTool(InterpretedTool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${args[0]} + ${args[1]} with addition=${addition}"
    t = MyTool()
    assert t.get_command(
        1, 2, addition="yeah") == " 1 + 2 with addition=yeah"


def test_interpreter_tool_returns_method_none():
    class MyTool(InterpretedTool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${args[0]} + ${args[1]} with addition=${addition}"
        returns = None

    t = MyTool()
    assert t._returns() is None


def test_interpreter_tool_returns_method_string():
    class MyTool(InterpretedTool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${args[0]} + ${args[1]} with addition=${addition}"
        returns = "testme"

    t = MyTool()
    assert t._returns() == "testme"


def test_interpreter_tool_returns_method_function():
    class MyTool(InterpretedTool):
        name = "Mytool"
        interpreter = "bash"
        command = " ${args[0]} + ${args[1]} with addition=${addition}"
        returns = lambda t: "testme"

    t = MyTool()
    assert t._returns() == "testme"


def test_on_start_listener_calls():
    called = [False]

    def my_start(tool):
        called[0] = True

    class MyTool(Tool):
        name = "Mytool"
        on_start = [my_start]

        def call(self):
            pass

    t = MyTool()
    t()
    assert called[0] is True
