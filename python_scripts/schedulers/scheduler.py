# pylint: disable=invalid-name

from abc import ABC, abstractmethod
from collections import defaultdict, namedtuple
import subprocess
from string import Template
import os

TestData = namedtuple(
    "TestData",
    [
        "mypath",
        "b_filename",
        "t_filename",
        "machine_name",
        "script_dir",
        "artifacts_root",
        "fb",
        "ft",
    ],
)

MonitorData = namedtuple(
    "MonitorData",
    [
        "path_",
        "job_number",
        "sub_directory",
        "machine_name",
        "scheduler_type",
        "script_directory",
        "artifacts_root",
        "mpi_version",
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

    @abstractmethod
    def createHeaders(self, test: TestData):
        for _file_handler in [test.fb, test.ft]:
            _create_headers(self.template_data, _file_handler, self.TEMPLATE_PATH)

    @abstractmethod
    def submitJob(self, test, subdir, mpiver, branch):
        _data = MonitorData(
            path_=test.mypath,
            build_filename=test.b_filename,
            test_filename=test.t_filename,
            job_number=self.fetch_job_number(test.b_filename),
            sub_directory=subdir,
            machine_name=test.machine_name,
            scheduler_type=self.type,
            script_directory=test.script_dir,
            artifacts_root=test.artifacts_root,
            mpi_version=mpiver,
            branch=branch,
            dryrun=False,
        )
        self._submit_job(_data)

    def _submit_job(self, _data: MonitorData):
        monitor_cmd_build = monitor_build(_data)
        result_job_number = batch_test(_data.job_number, _data.test_filename)
        monitor_cmd_test = monitor_test(_data._replace(job_number=result_job_number))

        self.create_get_res_scripts(
            monitor_cmd_build, monitor_cmd_test, "insert_default_bash"
        )

    @abstractmethod
    def checkQueue(self):
        raise NotImplementedError()

    @classmethod
    def create_get_res_scripts(cls, monitor_cmd_build, monitor_cmd_test, bash):
        # write these out no matter what, so we can run them manually, if necessary
        with open("getres-build.sh", "w") as get_res_file:
            get_res_file.write(f"#!{bash} -l\n")
            get_res_file.write(f"{monitor_cmd_build} >& build-res.log &\n")
            os.system("chmod +x getres-build.sh")

        with open("getres-test.sh", "w") as get_res_file:
            get_res_file.write(f"#!{bash} -l\n")
            get_res_file.write(f"{monitor_cmd_test} >& test-res.log &\n")
            os.system("chmod +x getres-test.sh")

    @classmethod
    def fetch_job_number(cls, filename):
        try:
            return (
                subprocess.check_output(f"sbatch {filename}", shell=True)
                .strip()
                .decode("utf-8")
                .split()[3]
            )
        except subprocess.CalledProcessError as error:
            raise ValueError from error


class scheduler(Scheduler):
    """scheduler is a wrapper around Scheduler to maintain backwards compatibility"""


def monitor_build(_data: MonitorData):
    # External Call
    monitor_cmd_build = f"python3 {_data.path_}/archive_results.py -j {_data.job_number} -b {_data.sub_directory} -m {_data.machine_name} -s {_data.scheduler_type} -t {_data.script_directory} -a {_data.artifacts_root} -M {_data.mpi_version} -B {_data.branch} -d {_data.dryrun}"

    if _data.dryrun is True:
        print(monitor_cmd_build)
    else:
        subprocess.Popen(
            monitor_cmd_build,
            shell=True,
            stdin=None,
            stdout=None,
            stderr=None,
            close_fds=True,
        )

    return monitor_cmd_build


def monitor_test(_data: MonitorData):
    # External Call
    monitor_cmd_test = f"python3 {_data.path_}/archive_results.py -j {_data.job_number} -b {_data.sub_directory} -m {_data.machine_name} -s {_data.scheduler_type} -t {_data.script_directory} -a {_data.artifacts_root} -M {_data.mpi_version} -B {_data.branch} -d {_data.dryrun}"

    if _data.dryrun is True:
        print(monitor_cmd_test)
    else:
        subprocess.Popen(
            monitor_cmd_test,
            shell=True,
            stdin=None,
            stdout=None,
            stderr=None,
            close_fds=True,
        )
    return monitor_cmd_test


def batch_test(job_number, test_filename):
    # submit the second job to be dependent on the first
    batch_test_cmd = f"sbatch --depend=afterok:{job_number} {test_filename}"
    print(f"Submitting test_batch with command: {batch_test_cmd}")
    return Scheduler.fetch_job_number(test_filename)


def _create_headers(template_data, file_handler, template_path):
    result = None
    with open(template_path, "r") as _template:
        src = Template(_template.read())
        result = src.safe_substitute(template_data)
        file_handler.writelines(result)
