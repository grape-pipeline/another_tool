#!/usr/bin/env python
from another.cluster import Slurm, Feature
from another import Tool


class MyTool(Tool):
    name = "Mytool"

    def call(self, a, b):
        return a + b


class MyFailingTool(Tool):
    name = "Mytool"

    def call(self, a, b):
        raise ValueError("Something went wrong!")
        return a + b


def tests_slurm_submission():
    slurm = Slurm(sbatch="/opt/perf/bin/sbatch")
    tool = MyTool()
    feature = tool.submit(slurm, args=([1, 3],),
                          queue="development,debug", max_time=1,
                          extra=['-A', 'FB', '-C', 'FB'])
    assert feature is not None
    assert feature.jobid is not None
    assert feature.stdout is not None
    assert feature.stderr is not None
    assert feature.stdout[0] == "/"
    assert feature.stderr[0] == "/"


def test_slurm_get_result_from_feature():
    slurm = Slurm(sbatch="/opt/perf/bin/sbatch")
    tool = MyTool()
    feature = tool.submit(slurm, args=([1, 3],),
                          queue="development,debug", max_time=1,
                          extra=['-A', 'FB', '-C', 'FB'])
    assert feature is not None
    assert feature.get(slurm, check_interval=5) == 4


def test_slurm_get_result_exception():
    slurm = Slurm(sbatch="/opt/perf/bin/sbatch")
    tool = MyFailingTool()
    feature = tool.submit(slurm, args=([1, 3],),
                          queue="debug", max_time=1,
                          extra=['-A', 'FB', '-C', 'ExFB'])
    assert feature is not None
    try:
        feature.get(slurm, check_interval=5)
        assert False
    except Exception, e:
        assert str(e) == "Something went wrong!", str(e)


def test_slurm_load_result_exception():
    r = Feature(1)._load_results("test_data/result_exception.out")
    assert str(r) == "Something went wrong!"


def test_load_results():
    assert Feature(1)._load_results("test_data/result_4.out") == 4
