import subprocess
from collections import namedtuple, defaultdict
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

TEMPLATE_PATH = "../templates/build.bat.template"


class Slurm:
    def __init__(self):
        self.type = "slurm"
        self.data = defaultdict(str)

    @property
    def template_data(self):
        if not self.data:
            return {}
        return {
            "filename": self.data.get("filename", ""),
            "partition": format_partition(self.data.get("partition", "")),
            "account": self.data.get("account", ""),
            "time": self.data.get("time", ""),
            "cluster": format_cluster(self.data.get("cluster", "")),
            "constraint": format_constraint(self.data.get("constraint", "")),
            "queue": self.data.get("queue", ""),
            "cpn": self.data.get("cpn", ""),
        }

    def createHeaders(self, test: TestData):  # pylint disable=invalid_name
        for _file_handler in [test.fb, test.ft]:
            _create_headers(self.template_data, _file_handler)

    def submitJob(self, test, subdir, mpiver, branch):
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

    def parse_test_data(self, test):
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

    def checkqueue(self, jobid):
        return _check_queue(jobid)


class slurm(Slurm):
    """slurm is a wrapper around Slurm to maintain backwards compatibility"""


def _create_headers(template_data, file_handler):
    result = None
    with open(TEMPLATE_PATH, "r") as _template:
        src = Template(_template.read())
        result = src.safe_substitute(template_data)
        file_handler.writelines(result)


def _submit_job(_data: MonitorData):
    monitor_cmd_build = monitor_build(_data)
    result_job_number = batch_test(_data.job_number, _data.test_filename)
    monitor_cmd_test = monitor_test(_data._replace(job_number=result_job_number))

    # External Call
    createGetResScripts(monitor_cmd_build, monitor_cmd_test, "insert_default_bash")


def _check_queue(jobid):
    if int(jobid) < 0:
        return True
    queue_query = (
        f"sacct -j {jobid} | head -n 3 | tail -n 1 | awk -F ' ' '{{print $6}}'"
    )
    try:
        result = (
            subprocess.check_output(queue_query, shell=True).strip().decode("utf-8")
        )
        return result in ["COMPLETED", "TIMEOUT", "FAILED", "CANCELLED"]
    except subprocess.CalledProcessError:
        return True


def createGetResScripts(monitor_cmd_build, monitor_cmd_test, bash):
    # write these out no matter what, so we can run them manually, if necessary
    with open("getres-build.sh", "w") as get_res_file:
        get_res_file.write("#!{} -l\n".format(bash))
        get_res_file.write(f"{monitor_cmd_build} >& build-res.log &\n")
        os.system("chmod +x getres-build.sh")

    with open("getres-test.sh", "w") as get_res_file:
        get_res_file.write("#!{} -l\n".format(bash))
        get_res_file.write("{} >& test-res.log &\n".format(monitor_cmd_test))
        os.system("chmod +x getres-test.sh")


def format_constraint(value):
    if not value:
        return ""
    return f"SBATCH -C {value}"


def format_partition(value):
    if not value:
        return ""
    return f"#SBATCH --partition={value}"


def format_cluster(value):
    # SBATCH --cluster={}\n
    if not value:
        return ""
    return f"#SBATCH --cluster={value}"


def fetch_job_number(filename):
    try:
        return (
            subprocess.check_output(f"sbatch {filename}", shell=True)
            .strip()
            .decode("utf-8")
            .split()[3]
        )
    except subprocess.CalledProcessError:
        raise ValueError("Unable to determine job number from file %s", filename)


def monitor_build(_data: MonitorData):
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
    return (
        subprocess.check_output(batch_test_cmd, shell=True)
        .strip()
        .decode("utf-8")
        .split()[3]
    )


if __name__ == "__main__":
    pass
