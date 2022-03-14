"""
Parses and hydrates python string templates
"""

import collections
import os
import logging
import pathlib
import re
import subprocess
import abc
from typing import List

from job.request import JobRequest


DEFAULT_ENCODING = "utf-8"
DEFAULT_BASH = "/bin/bash"

JobProperties = collections.namedtuple(
    "JobProperties", ["filename", "queue", "cpn", "time", "partition", "cluster"]
)

defaults = {
    "machine_name": None,
    "git-https": True,  # always true
    "https": True,  # always true
    "bash": "/bin/bash",
    "account": None,
    "partition": "none",
    "queue": None,
    "headnodename": f"{os.uname()[1]}",
    "nuopcbranch": "develop",
    "build_types": ["O", "g"],  # static
    "script_dir": os.getcwd(),
    "cluster": None,
    "constraint": None,
    "subdir": "",
    "filename": "build-intel_18.0.5_intelmpi_O.bat",  # TODO
}


class Scheduler(abc.ABC):
    """represents instruction header"""

    job_id: str
    bash: str

    def __init__(self, request: JobRequest):
        self.request = request
        self.bash = "/usr/bin/bash"
        self.hydrate()

    def __getitem__(self, key):
        try:
            return self.request.__getattr__(key)
        except AttributeError:
            return defaults[key]

    def __getattr__(self, key):
        try:
            return self.request.__getattr__(key)
        except AttributeError:
            return defaults[key]

    @abc.abstractmethod
    def hydrate(self):
        """populates the template with data"""
        raise NotImplementedError

    @classmethod
    def queue_query(cls, **kwds):
        """cli command to query the job queue"""
        raise NotImplementedError

    def bash_header_line(self) -> str:
        """default bash header line of #!/usr/bin/derp/derp"""
        return f"#!{self.bash} -l\n"

    def text(self) -> str:
        """returns header as string with '\n'"""
        return "\n".join(
            [
                self.bash_header_line(),
                "export JOBID=$1",
            ]
        )

    @classmethod
    def from_type(cls, scheduler_type: str, props: JobProperties) -> "Scheduler":
        """factory for making schedulers"""
        _map = {
            "pbs": PBS(**props._asdict()),
            "slurm": Slurm(**props._asdict()),
        }
        return _map[scheduler_type.lower()](props)


class Slurm(Scheduler):
    """slurm scheduler instructions"""

    @classmethod
    def queue_query(cls, **kwds):
        """returns the job scheduler queue query command"""
        if "job_id" not in kwds:
            raise ValueError("requires job_id")
        return f"qstat -H {kwds['job_id']} | tail -n 1 | awk -F ' +' '{{print $10}}'"

    def text(self) -> str:
        return self.bash_header_line() + self.hydrate()

    def hydrate(self) -> str:
        """hydrates the template with data"""
        return f"""
#SBATCH --account={account}
#SBATCH -o {self.filename}_%j.o
#SBATCH -e test-intel_2020_intelmpi_g.bat_%j.e
#SBATCH --time={self.time}
#SBATCH --partition={self.partition}
#SBATCH --cluster={self.cluster}
#SBATCH --qos={self.queue}
#SBATCH --nodes=1
#SBATCH --ntasks-per-node={self.cpn}
#SBATCH --exclusive
export JOBID=$SLURM_JOBID
    """


class PBS(Scheduler):
    """slurm scheduler instructions"""

    @classmethod
    def queue_query(cls, **kwds):
        """returns the job scheduler queue query command"""
        if "job_id" not in kwds:
            raise ValueError("requires job_id")
        return f"qstat -H {kwds['jobid']} | tail -n 1 | awk -F ' +' '{{print $10}}'"

    def text(self) -> str:
        return self.bash_header_line() + self.hydrate()

    def hydrate(self) -> str:
        """hydrates the template with data"""
        return f"""
#PBS -N {self.filename}
#PBS -l walltime={self.time}
#PBS -q {self.queue}
#PBS -A {account}
#PBS -l select=1:ncpus={self.cpn}:mpiprocs={self.cpn}
JOBID="`echo $PBS_JOBID | cut -d. -f1`\n\n"
cd {os.getcwd()}\n
    """


def from_job_request(request: JobRequest):
    """factory"""
    _map = {"pbs": PBS, "slurm": Slurm}
    return _map[str(request.scheduler)](request)


def account() -> str:
    """returns the users local linux account id"""
    # TODO Linux get account ID
    raise NotImplementedError


JobNumber = int


def run_batch(cmd) -> JobNumber:
    """runs a job using shell.  returns JobNumber"""
    return int(_run_with_output(cmd).strip().split(".", maxsplit=1)[0])


JobRequestProps = collections.namedtuple(
    "JobRequestProps",
    [
        "mypath",
        "jobnum",
        "subdir",
        "machine_name",
        "type",
        "script_dir",
        "artifacts_root",
        "mpiver",
        "branch",
        "dryrun",
        "b_filename",
        "t_filename",
    ],
)


Command = collections.namedtuple("Command", ["type", "string"])
CommandString = str


def submitJob(props: JobRequestProps) -> None:
    """submits jobs to the job scheduler"""
    build_command = Command("build", f"qsub {props.b_filename}")
    build_job_number = submit_job(props, build_command)
    test_command = Command(
        "test", f"qsub -W depend=afterok:{build_job_number} {props.t_filename}"
    )
    submit_job(
        props,
        test_command,
    )


def submit_job(props: JobRequestProps, cmd: Command) -> JobNumber:
    """submits a job request and monitors the output based on job number"""
    logging.debug("running cmd [%s]", cmd)
    jobnum = 1234 if props.dryrun is False else run_batch(cmd)
    logging.debug("[%s] submitted [%s]", cmd, jobnum)
    monitor_cmd = Command("build", _get_monitor_cmd(jobnum, props))
    logging.debug(monitor_cmd.string)
    if not props.dryrun:
        _run_no_output(monitor_cmd.string)

    create_rescue_script([monitor_cmd])
    return jobnum


def checkqueue(job_id):
    queue_query = f"qstat -H {job_id} | tail -n 1 | awk -F ' +' '{{print $10}}'"
    try:
        if _run_with_output(queue_query).upper() == "F":
            return True
    except Exception as e:
        logging.info("What is this error type?  We need to except it.. [%s]", str(e))
        return True
    return False


CommandType = str
CommandString = str


def create_rescue_script(commands: List[Command]) -> None:
    """writes out rescue scripts"""
    for _type, command in commands:
        file_path = pathlib.Path(f"./getres-{_type}.sh")
        with open(file_path, encoding=DEFAULT_ENCODING) as _file:
            _file.write("#!{} -l\n".format(DEFAULT_BASH))
            _file.write(f"{command} >& {_type}-res.log &\n")
        _make_executable(file_path)


def _make_executable(_path: pathlib.Path) -> int:
    return os.system(f"chmod +x {_path}")


def _get_monitor_cmd(jobnum: JobNumber, props: JobRequestProps) -> str:
    return f"python3 {props.mypath}/archive_results.py -j {jobnum} -b {props.subdir} -m {props.machine_name} -s {props.script_dir} -t {props.type} -a {props.artifacts_root} -M {props.mpiver} -B {props.branch} -d {props.dryrun}"


def _run_no_output(cmd: str) -> None:
    """runs a monitor command."""
    subprocess.Popen(
        cmd,
        shell=True,
        stdin=None,
        stdout=None,
        stderr=None,
        close_fds=True,
    )


def _run_with_output(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True).decode("utf-8")


def update_repo(props: JobRequestProps, subdir, branch, nuopcbranch):

    cmdstring = f"git clone -b {branch} git@github.com:esmf-org/esmf {subdir}"
    nuopcclone = (
        f"git clone -b {nuopcbranch} git@github.com:esmf-org/nuopc-app-prototypes"
    )

    if props.dryrun == True:
        print("would have executed {}".format(cmdstring))
        print("would have executed {}".format(nuopcclone))
        print("would have cd'd to {}".format(subdir))

    else:
        status = _run_with_output(cmdstring)
        os.chdir(subdir)
        _run_no_output("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
        _run_no_output(f"git checkout {branch}")
        _run_no_output(f"git pull origin {branch}")
        status = _run_with_output(nuopcclone)
        logging.debug("status from nuopc clone command %s was %s", nuopcclone, status)


Data = collections.namedtuple(
    "Data", ["build_types", "machine_list", "key", "scheduler_type"]
)


def subdir(comp, ver, key, build_type, branch):
    """generates subdir"""
    return re.sub("/", "_", f"{comp}_{ver}_{key}_{build_type}_{branch}")


def create_and_submit(data: Data):
    """create job"""

    machine_list = data.machine_list
    key = data.key

    props = JobProperties(
        filename="", queue="", cpn="", time="", partition="", cluster=""
    )
    scheduler = Scheduler.from_type(data.scheduler_type, props)

    # Create
    # scheduler.createHeaders(self)
    # createScripts(build_type, comp, ver, mpidict, mpitypes, key, branch)

    # Submit
    # scheduler.submitJob(self, subdir, mpiver, branch)
    # os.chdir("..")


def createScripts(self, build_type, comp, ver, mpidict, key):
    mpiflavor = mpidict[key]
    headerList = ["build", "test"]
    if "pythontest" in mpiflavor:
        headerList = ["build", "test", "python"]
    for headerType in headerList:
        if headerType == "build":
            file_out = self.fb
        elif headerType == "test":
            file_out = self.ft
        else:
            pythonscript = open("runpython.sh", "w")
            file_out = pythonscript
            file_out.write("#!{} -l\n".format(self.bash))
            file_out.write("cd {}\n".format(os.getcwd()))
            file_out.write(
                "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n"
            )
            file_out.write("cd {}/src/addon/ESMPy\n".format(os.getcwd()))
        if "unloadmodule" in self.machine_list[comp]:
            file_out.write(
                "\nmodule unload {}\n".format(self.machine_list[comp]["unloadmodule"])
            )
        if "modulepath" in self.machine_list:
            modulepath = self.machine_list["modulepath"]
            file_out.write("\nmodule use {}\n".format(self.machine_list["modulepath"]))
        if "extramodule" in self.machine_list[comp]:
            file_out.write(
                "\nmodule load {}\n".format(self.machine_list[comp]["extramodule"])
            )

        if mpiflavor["module"] == "None":
            mpiflavor["module"] = ""
            cmdstring = (
                "export ESMF_MPIRUN={}/src/Infrastructure/stubs/mpiuni/mpirun\n".format(
                    os.getcwd()
                )
            )
            file_out.write(cmdstring)

        if "mpi_env_vars" in mpidict[key]:
            for mpi_var in mpidict[key]["mpi_env_vars"]:
                file_out.write(
                    "export {}\n".format(mpidict[key]["mpi_env_vars"][mpi_var])
                )

        if self.machine_list[comp]["versions"][ver]["netcdf"] == "None":
            modulecmd = "module load {} {} \n\n".format(
                self.machine_list[comp]["versions"][ver]["compiler"],
                mpiflavor["module"],
            )
            esmfnetcdf = "\n"
            file_out.write(modulecmd)
        else:
            modulecmd = "module load {} {} {}\n".format(
                self.machine_list[comp]["versions"][ver]["compiler"],
                mpiflavor["module"],
                self.machine_list[comp]["versions"][ver]["netcdf"],
            )
            esmfnetcdf = "export ESMF_NETCDF=nc-config\n\n"
            file_out.write(modulecmd)

        if "hdf5" in self.machine_list[comp]["versions"][ver]:
            modulecmd = "module load {} \n".format(
                self.machine_list[comp]["versions"][ver]["hdf5"]
            )
            file_out.write(modulecmd)
        if "netcdf-fortran" in self.machine_list[comp]["versions"][ver]:
            modulecmd = "module load {} \n".format(
                self.machine_list[comp]["versions"][ver]["netcdf-fortran"]
            )
            file_out.write(modulecmd)

        if headerType == "build":
            file_out.write("module list >& module-build.log\n\n")
        elif headerType == "test":
            file_out.write("module list >& module-test.log\n\n")

        file_out.write("set -x\n")
        file_out.write(esmfnetcdf)

        if "extra_env_vars" in self.machine_list[comp]["versions"][ver]:
            for var in self.machine_list[comp]["versions"][ver]["extra_env_vars"]:
                file_out.write(
                    "export {}\n".format(
                        self.machine_list[comp]["versions"][ver]["extra_env_vars"][var]
                    )
                )

        if "extra_commands" in self.machine_list[comp]["versions"][ver]:
            for cmd in self.machine_list[comp]["versions"][ver]["extra_commands"]:
                file_out.write(
                    "{}\n".format(
                        self.machine_list[comp]["versions"][ver]["extra_commands"][cmd]
                    )
                )

        cmdstring = "export ESMF_DIR={}\n".format(os.getcwd())
        file_out.write(cmdstring)

        cmdstring = "export ESMF_COMPILER={}\n".format(comp)
        file_out.write(cmdstring)

        cmdstring = "export ESMF_COMM={}\n".format(key)
        file_out.write(cmdstring)

        cmdstring = "export ESMF_BOPT='{}'\n".format(build_type)
        file_out.write(cmdstring)

        cmdstring = "export ESMF_TESTEXHAUSTIVE='ON'\n"
        file_out.write(cmdstring)

        cmdstring = "export ESMF_TESTWITHTHREADS='ON'\n"
        file_out.write(cmdstring)

        if headerType == "build":

            cmdstring = "make -j {} 2>&1| tee build_$JOBID.log\n\n".format(self.cpn)
            file_out.write(cmdstring)
        elif headerType == "test":
            cmdstring = "make info 2>&1| tee info.log \nmake install 2>&1| tee install_$JOBID.log \nmake all_tests 2>&1| tee test_$JOBID.log \n"
            file_out.write(cmdstring)
            #       file_out.write("ssh {} {}/{}/getres-int.sh\n".format(self.headnodename,self.script_dir,os.getcwd()))
            cmdstring = (
                "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n"
            )
            file_out.write(cmdstring)
            if mpiflavor["module"] != "None":
                cmdstring = "chmod +x runpython.sh\ncd nuopc-app-prototypes\n./testProtos.sh 2>&1| tee ../nuopc_$JOBID.log \n\n"
                file_out.write(cmdstring)
        #         file_out.write("ssh {} {}/{}/getres-int.sh\n".format(self.headnodename,self.script_dir,os.getcwd()))
        else:
            cmdstring = "python3 setup.py test_examples_dryrun\npython3 setup.py test_regrid_from_file_dryrun\n"
            file_out.write(cmdstring)
        #       file_out.write("ssh {} {}/{}/getres-int.sh\n".format(self.headnodename,self.script_dir,os.getcwd()))

        if ("pythontest" in mpiflavor) and (headerType == "test"):

            cmdstring = "\ncd ../src/addon/ESMPy\n"
            file_out.write(cmdstring)
            cmdstring = "\nexport PATH=$PATH:$HOME/.local/bin\n".format(os.getcwd())
            file_out.write(cmdstring)
            cmdstring = "python3 setup.py build 2>&1 | tee python_build.log\n".format(
                self.headnodename
            )
            file_out.write(cmdstring)
            cmdstring = "ssh {} {}/runpython.sh 2>&1 | tee python_build.log\n".format(
                self.headnodename, os.getcwd()
            )
            file_out.write(cmdstring)
            cmdstring = "python3 setup.py test 2>&1 | tee python_test.log\n".format(
                self.headnodename
            )
            file_out.write(cmdstring)
            cmdstring = "python3 setup.py test_examples 2>&1 | tee python_examples.log\n".format(
                self.headnodename
            )
            file_out.write(cmdstring)
            cmdstring = "python3 setup.py test_regrid_from_file 2>&1 | tee python_regrid.log\n".format(
                self.headnodename
            )
            file_out.write(cmdstring)
        file_out.close()
        mpimodule = mpiflavor["module"]
        if mpimodule == "":
            self.mpiver = "None"
        else:
            self.mpiver = mpiflavor["module"].split("/")[-1]


def createGetResScripts(self, monitor_cmd_build, monitor_cmd_test):
    # write these out no matter what, so we can run them manually, if necessary
    get_res_file = open("getres-build.sh", "w")
    get_res_file.write("#!{} -l\n".format(self.bash))
    get_res_file.write("{} >& build-res.log &\n".format(monitor_cmd_build))
    get_res_file.close()
    os.system("chmod +x getres-build.sh")

    get_res_file = open("getres-test.sh", "w")
    get_res_file.write("#!{} -l\n".format(self.bash))
    get_res_file.write("{} >& test-res.log &\n".format(monitor_cmd_test))
    get_res_file.close()
    os.system("chmod +x getres-test.sh")


def createJobCardsAndSubmit(self):
    for build_type in self.build_types:
        for comp in self.machine_list["compiler"]:
            for ver in self.machine_list[comp]["versions"]:
                print("{}".format(self.machine_list[comp]["versions"][ver]["mpi"]))
                mpidict = self.machine_list[comp]["versions"][ver]["mpi"]
                mpitypes = mpidict.keys()
                print(self.machine_list[comp]["versions"][ver])
                for key in mpitypes:
                    if "build_time" in self.machine_list[comp]:
                        self.build_time = self.machine_list[comp]["build_time"]
                    else:
                        self.build_time = "1:00:00"
                    if "test_time" in self.machine_list[comp]:
                        self.test_time = self.machine_list[comp]["test_time"]
                    else:
                        self.test_time = "1:00:00"
                    for branch in self.machine_list["branch"]:
                        if "nuopcbranch" in self.machine_list:
                            nuopcbranch = self.machine_list["nuopcbranch"]
                        else:
                            nuopcbranch = branch
                        subdir = "{}_{}_{}_{}_{}".format(
                            comp, ver, key, build_type, branch
                        )
                        subdir = re.sub(
                            "/", "_", subdir
                        )  # Some branches have a slash, so replace that with underscore
                        if self.https == True:
                            cmdstring = "git clone -b {} https://github.com/esmf-org/esmf {}".format(
                                branch, subdir
                            )
                            nuopcclone = "git clone -b {} https://github.com/esmf-org/nuopc-app-prototypes".format(
                                nuopcbranch
                            )
                        else:
                            cmdstring = "git clone -b {} git@github.com:esmf-org/esmf {}".format(
                                branch, subdir
                            )
                            nuopcclone = "git clone -b {} git@github.com:esmf-org/nuopc-app-prototypes".format(
                                nuopcbranch
                            )
                        self.updateRepo(subdir, branch, nuopcbranch)
                        # self.scheduler.createHeaders(self)
                        # self.createScripts(
                        #     build_type, comp, ver, mpidict, mpitypes, key, branch
                        # )
                        # self.scheduler.submitJob(self, subdir, self.mpiver, branch)
                        # os.chdir("..")


def batch_filename(_type, comp, ver, key, build_type):
    return f"{_type}-{comp}_{ver}_{key}_{build_type}.bat".format(
        comp, ver, key, build_type
    )
