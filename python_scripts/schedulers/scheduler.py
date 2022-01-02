# pylint: disable=invalid-name, unspecified-encoding

import logging
import os
import subprocess
from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
from string import Template
from test import TestData

from archive_results import ArchiveResults, ArchiveResultsData

MonitorData = namedtuple(
    "MonitorData",
    [
        "path_",
        "job_id",
        "build_basename",
        "machine_name",
        "scheduler",
        "test_root_dir",
        "artifacts_root",
        "mpiversion",
        "branch",
        "dryrun",
        "test_filename",
        "build_filename",
    ],
)


class Scheduler(ABC):

    SCHEDULER_TYPE = "NOT IMPLEMENTED"
    TEMPLATE_PATH = "NOT IMPLEMENTED"

    def __init__(self):
        self.type = self.SCHEDULER_TYPE
        self.data = defaultdict(str)

    @property
    @abstractmethod
    def template_data(self):
        raise NotImplementedError()

    def create_headers(self, test: TestData):
        for _file_handler in [test.fb, test.ft]:
            _create_headers(self.template_data, _file_handler, self.TEMPLATE_PATH)

    def submit_job(self, test: TestData, subdir, mpiver, branch):
        _data = MonitorData(
            path_=test.mypath,
            build_filename=test.b_filename,
            test_filename=test.t_filename,
            job_id=self.fetch_job_number(test.b_filename),
            build_basename=subdir,
            machine_name=test.machine_name,
            scheduler=self.type,
            test_root_dir=test.script_dir,
            artifacts_root=test.artifacts_root,
            mpiversion=mpiver,
            branch=branch,
            dryrun=False,
        )
        self.monitor(_data)
        result_job_number = self._batch_test(_data.job_id, _data.test_filename)
        self.monitor(_data._replace(job_id=result_job_number))
        self.create_get_res_scripts(_data, "insert_default_bash")

    @abstractmethod
    def check_queue(self, job_id):
        raise NotImplementedError()

    @classmethod
    def monitor(cls, _data: MonitorData):
        _monitor(_data)

    @classmethod
    def generate_monitor_command(cls, _data):
        return f"python3 {_data.path_}/archive_results.py -j {_data.job_id} -b {_data.build_basename} -m {_data.machine_name} -s {_data.scheduler} -t {_data.test_root_dir} -a {_data.artifacts_root} -M {_data.mpiversion} -B {_data.branch} -d {_data.dryrun}"

    @classmethod
    def create_get_res_scripts(cls, _data, bash) -> None:
        monitor_cmd = cls.generate_monitor_command(_data)
        # write these out no matter what, so we can run them manually, if necessary
        for _type in ["build", "test"]:
            with open(f"getres-{_type}.sh", "w") as get_res_file:
                lines = [f"#!{bash} -l", f"{monitor_cmd} >& {_type}-res.log &"]
                get_res_file.writelines(lines)
                os.system(f"chmod +x getres-{_type}.sh")

    @classmethod
    def fetch_job_number(cls, filename) -> str:
        try:
            return (
                subprocess.check_output(f"sbatch {filename}", shell=True)
                .strip()
                .decode("utf-8")
                .split()[3]
            )
        except subprocess.CalledProcessError as error:
            raise ValueError from error

    def _batch_test(self, job_number, test_filename) -> str:
        # submit the second job to be dependent on the first
        batch_test_cmd = f"sbatch --depend=afterok:{job_number} {test_filename}"
        print(f"Submitting test_batch with command: {batch_test_cmd}")
        return Scheduler.fetch_job_number(test_filename)


class scheduler(Scheduler):
    """scheduler is a wrapper around Scheduler to maintain backwards compatibility"""


def _create_headers(template_data, file_handler, template_path) -> None:
    result = None
    with open(template_path, "r") as _template:
        src = Template(_template.read())
        result = src.safe_substitute(template_data)
        file_handler.writelines(result)


def _monitor(_data: MonitorData):
    if _data.dryrun is True:
        logging.info("emulating archive running")
    else:
        archiver = ArchiveResults(ArchiveResultsData(*_data._asdict()))
        archiver.monitor()
