# pylint: disable=unspecified-encoding

import os
from string import Template
import subprocess
from collections import namedtuple, defaultdict


TEMPLATE_PATH = "../templates/pbs.build.bat.template"

SCHEDULER_TYPE = "slurm"

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

TemplateData = namedtuple(
    "TemplateData",
    [
        "b_filename",
        "t_filename",
        "build_time",
        "test_time",
        "filename_and_time",
        "queue",
        "account",
        "cpn",
        "cwd",
    ],
)

ScriptType = namedtuple("ScriptType", ["handler", "type"])


class PBS:
    def __init__(self, scheduler_type):
        self.type = scheduler_type
        self.data = defaultdict(str)

    @property
    def template_data(self) -> TemplateData:
        # May not need empty strings for defaults with defaultdict
        return TemplateData(
            b_filename=self.data.get("b_filename", ""),
            t_filename=self.data.get("t_filename", ""),
            build_time=self.data.get("build_time", ""),
            test_time=self.data.get("test_time", ""),
            filename_and_time="",
            queue=self.data.get("queue", ""),
            account=self.data.get("account", ""),
            cpn=self.data.get("cpn", ""),
            cwd=os.getcwd(),
        )

    def _parse_test_data(self, test):
        return TestData(
            mypath=test.mypath,
            b_filename=test.b_filename,
            t_filename=test.t_filename,
            machine_name=test.machine_name,
            script_dir=test.script_dir,
            artifacts_root=test.artifacts_root,
            fb=test.fb,
            ft=test.ft,
        )

    def createHeaders(self, test: TestData):  # pylint: disable=invalid-name
        for file_info in [
            ScriptType(test.fb, "build"),
            ScriptType(test.ft, "test"),
        ]:
            _create_headers(self.template_data, file_info)

    def submitJob(self, test, subdir, mpiver, branch):  # pylint: disable=invalid-name
        _data = MonitorData(
            path_=test.mypath,
            build_filename=test.b_filename,
            test_filename=test.t_filename,
            job_number=fetch_job_number(test.b_filename),
            sub_directory=subdir,
            machine_name=test.machine_name,
            scheduler_type=self.type,
            script_directory=test.script_dir,
            artifacts_root=test.artifacts_root,
            mpi_version=mpiver,
            branch=branch,
            dryrun=False,
        )
        _submit_job(_data)

    def checkqueue(self, jobid):  # pylint: disable=invalid-name
        return _check_queue(jobid)


class pbs(PBS):  # pylint: disable=invalid-name
    """pbs is a wrapper around PBS to maintain backwards compatibility"""


def _submit_job(_data: MonitorData):
    monitor_cmd_build = monitor_build(_data)
    result_job_number = batch_test(_data.job_number, _data.test_filename)
    monitor_cmd_test = monitor_test(_data._replace(job_number=result_job_number))

    create_get_res_scripts(monitor_cmd_build, monitor_cmd_test, "insert_default_bash")


def create_get_res_scripts(monitor_cmd_build, monitor_cmd_test, bash):
    # write these out no matter what, so we can run them manually, if necessary
    with open("getres-build.sh", "w") as get_res_file:
        get_res_file.write(f"#!{bash} -l\n")
        get_res_file.write(f"{monitor_cmd_build} >& build-res.log &\n")
        os.system("chmod +x getres-build.sh")

    with open("getres-test.sh", "w") as get_res_file:
        get_res_file.write(f"#!{bash} -l\n")
        get_res_file.write(f"{monitor_cmd_test} >& test-res.log &\n")
        os.system("chmod +x getres-test.sh")


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
    return fetch_job_number(test_filename)


def fetch_job_number(filename):
    try:
        return (
            subprocess.check_output(f"sbatch {filename}", shell=True)
            .strip()
            .decode("utf-8")
            .split()[3]
        )
    except subprocess.CalledProcessError as error:
        raise ValueError from error


def _create_headers(template_data: TemplateData, file_info: ScriptType):
    file_handler, file_type = file_info

    def filename_and_time_template(filename, build_time):
        return f"""
        #PBS -N {filename}\n 
        #PBS -l walltime={build_time}\n
        """

    result = None
    with open(TEMPLATE_PATH, "r") as _template:
        if file_type == "build":
            template_data._replace(
                filename_and_time=filename_and_time_template(
                    template_data.b_filename, template_data.build_time
                )
            )
        else:
            template_data._replace(
                filename_and_time=filename_and_time_template(
                    template_data.t_filename, template_data.test_time
                )
            )
        src = Template(_template.read())
        result = src.safe_substitute(template_data._asdict())
        file_handler.writelines(result)


def _check_queue(job_id):
    # TODO abstract in root class
    if int(job_id) < 0:
        return True
    queue_query = f"qstat -H {job_id} | tail -n 1 | awk -F ' +' '{{print $10}}'"
    try:
        result = (
            subprocess.check_output(queue_query, shell=True).strip().decode("utf-8")
        )
        return result in ["F"]
    except subprocess.CalledProcessError:
        return True
