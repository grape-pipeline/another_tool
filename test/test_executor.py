#!/usr/bin/env python
"""Executor tests"""
from another import Tool
import another.executor as ex


class MyTool(Tool):
    name = "MyTool"

    def call(self, a, b):
        return a + b


def test_saving_simple_tool():
    t = MyTool()
    f = ex.dump(t)
    assert f is not None
