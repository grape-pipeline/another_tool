#!/usr/bin/env python
""" The JIP scripts module provides the facilities to parse jip scripts
"""
import os
import re

class ScriptError(Exception):
    """Default error raised by the script parser"""
    pass


class Script(object):
    """JIP script wrapper class"""
    def __init__(path):
        # the path to the script file
        self.script_file = path
        # ordered list of script command blocks
        self.commands = []
        # the script doctring
        self.doc_string = None


def split_header(lines):
    header = []
    content = []
    header_finished = False
    # iterate without shebang line
    for l in [l for l in lines if l[0:2] != "#!"]:
        if not header_finished and l[0] == "#" and l[0:2] not in ["#%"]:
            header.append(l)
        else:
            header_finished = True
            content.append(l)
    return header, content


def parse_blocks(content):
    current_block = []
    current_name = None
    current_args = None

    for l in content

def parse_script(path):
    if not os.path.exists(path):
        raise ScriptError("Script file not found : %s" % path)
    with open(path, 'r') as f:
        lines = [l.strip() for l in f.readlines() if len(l.strip()) > 0]
        header, content = split_header(lines)
        block = parse_blocks(content)
