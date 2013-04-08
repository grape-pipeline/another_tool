#!/usr/bin/env python
"""Test parts of the cluster implementation"""
from another import Tool
from another.cluster import Cluster, Slurm


class MyTool(Tool):
    name = "Mytool"

    def call(self, a, b):
        return a + b


def tests_slurm_submission():
    slurm = Slurm(sbatch="/opt/perf/bin/sbatch")
    tool = MyTool()
    print tool.submit(slurm, args=([1, 3],),
                      queue="development,debug", max_time=1,
                      extra=['-A', 'FB', '-C', 'FB'])


def test_saving_simple_tool():
    t = MyTool()
    ex = Cluster()
    f = ex.dump(t)
    assert f is not None
