#!/usr/bin/env python
"""The JIP python module allows to create batch processed computational
pipelines in python and execute them locally as well as running them
on a HPC grid
"""
__author__ = 'thasso'
__version__ = "1.0"


def discover():
    """Search loaded modules for tool implementation
    and returns a list of classes that are subclasses of
    :class:jip.tools.Tool

    :returns tools: list of Tool classes
    """
    from . import tools
    return tools.Tool.__subclasses__()
