# pylint: disable=unspecified-encoding

import argparse
from collections import namedtuple


import os
import pathlib
import re
import shutil
import subprocess
from typing import Tuple


import yaml

from schedulers.noscheduler import NoScheduler
from schedulers.pbs import pbs
from schedulers.slurm import slurm


def rmdir(path):
    shutil.rmtree(path)


ESMFTestData = namedtuple("ESMFTestData", ["yaml_file", "artifacts_root", "workdir", "dryrun"])


def namedtuple_with_defaults(typename, field_names, default_values=()):
    # Python 3.6
    T = namedtuple(typename, field_names)
    T.__new__.__defaults__ = (None,) * len(T._fields)
    if isinstance(default_values, dict):
        prototype = T(**default_values)
    else:
        prototype = T(*default_values)
    T.__new__.__defaults__ = tuple(prototype)
    return T


def uname():

    try:
        return os.uname()[1]
    except:
        return ""


GlobalProperties = namedtuple("GlobalProperties", ["reclone-artifacts"])

MachineProperties = namedtuple_with_defaults(
    "MachineProperties",
    [
        "bash",
        "account",
        "partition",
        "queue",
        "headnodename",
        "nuopcbranch",
        "corespernode",
        "scheduler",
        "cluster",
        "constraint",
        "git-https",
    ],
    {"bash": "/bin/bash", "headnodename": uname(), "nuopcbranch": "develop"},
)


class ESMFTest:
    build_types = ["O", "g"]

    def __init__(self, data: ESMFTestData):
        self.build_time = "1:00:00"
        self.test_time = "1:00:00"

        self._data = data
        self._scheduler = None
        self._mpiver = None

        self.global_properties, self.machine_properties = fetch_yaml_properties(
            local_yaml_config_path=data.yaml_file,
            global_yaml_config_path=self.global_yaml_config_path,
        )

    @property
    def scheduler(self):
        _map = {"slurm": slurm, "pbs": pbs, "None": NoScheduler}
        if self._scheduler is None:
            self._scheduler = _map[self.machine_properties.scheduler_type]()
        return self._scheduler

    @property
    def script_dir(self):
        return os.getcwd()

    @property
    def mypath(self):
        return pathlib.Path(__file__).parent.absolute()

    @property
    def global_yaml_config_path(self):
        return os.path.join(str(os.path.dirname(self._data.yaml_file)), "global.yaml")

    @property
    def dryrun(self):
        if self._data.dryrun:
            return True
        return False

    @property
    def https(self):
        return "git-https" in self.machine_properties

    @property
    def python_script_header(self):
        return [
            f"#!{self.machine_properties.bash} -l\n",
            f"cd {os.getcwd()}\n",
            "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n",
            f"cd {os.getcwd()}/src/addon/ESMPy\n",
        ]

    def runcmd(self, cmd):
        _runcmd(cmd, self.dryrun)

    def create_scripts(self, build_type, comp, ver, mpidict, key):
        mpiflavor = mpidict[key]
        for header_type in _get_header_list(mpidict[key]):
            is_python = header_type not in ["build", "test"]
            file_out_name = (
                _filename(header_type, comp, ver, key, build_type)
                if not is_python
                else "runpython.sh"
            )
            with open(file_out_name, "w") as file_out:
                if is_python:
                    file_out.writelines(self.python_script_header)

                if "unloadmodule" in self.machine_properties[comp]:
                    file_out.write(
                        f"\nmodule unload {self.machine_properties[comp]['unloadmodule']}\n"
                    )
                if "modulepath" in self.machine_properties:
                    file_out.write(f"\nmodule use {self.machine_properties['modulepath']}\n")
                if "extramodule" in self.machine_properties[comp]:
                    file_out.write(
                        f"\nmodule load {self.machine_properties[comp]['extramodule']}\n"
                    )

                if mpiflavor["module"] == "None":
                    mpiflavor["module"] = ""
                    file_out.write(
                        f"export ESMF_MPIRUN={os.getcwd()}/src/Infrastructure/stubs/mpiuni/mpirun\n"
                    )

                compiler_version = self.machine_properties[comp]["versions"][ver]
                netcdf = self.machine_properties[comp]["versions"][ver]["netcdf"]

                _write_mpi_environment_variables(mpidict[key], file_out)
                _write_netcdf(
                    netcdf,
                    mpiflavor["module"],
                    file_out,
                )

                if "hdf5" in compiler_version:
                    hdf5 = compiler_version["hdf5"]
                    file_out.write(f"module load {hdf5} \n")

                if "netcdf-fortran" in compiler_version:
                    netcdf_fortran = compiler_version["netcdf-fortran"]
                    file_out.write(f"module load {netcdf_fortran} \n")

                file_out.write(f"module list >& module-{header_type}.log\n\n")
                file_out.write("set -x\n")
                file_out.write("export ESMF_NETCDF=nc-config\n\n" if netcdf else "\n")

                _write_extra_env_vars(compiler_version, file_out)
                _write_extra_commands(compiler_version, file_out)
                _write_vars(file_out=file_out, build_type=build_type, comp=comp, key=key)

                if header_type == "build":

                    file_out.write(
                        f"make -j {self.machine_properties.cpn} 2>&1| tee build_$JOBID.log\n\n"
                    )
                elif header_type == "test":
                    lines = [
                        "make info 2>&1| tee info.log \nmake install 2>&1| tee install_$JOBID.log \nmake all_tests 2>&1| tee test_$JOBID.log \n",
                        "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n",
                    ]
                    file_out.writelines(lines)
                    if mpiflavor["module"] != "None":

                        file_out.write(
                            "chmod +x runpython.sh\ncd nuopc-app-prototypes\n./testProtos.sh 2>&1| tee ../nuopc_$JOBID.log \n\n"
                        )
                else:
                    file_out.write(
                        "python3 setup.py test_examples_dryrun\npython3 setup.py test_regrid_from_file_dryrun\n"
                    )
                if ("pythontest" in mpiflavor) and (header_type == "test"):
                    self._write_python_test(file_out)
                if mpiflavor["module"] != "":
                    self._mpiver = mpiflavor["module"].split("/")[-1]

    def create_job_card_and_submit(self):
        # TODO replace nested loops with itertools
        for build_type in self.build_types:
            for comp in self.machine_properties["compiler"]:
                for ver in self.machine_properties[comp]["versions"]:
                    print(f"{self.machine_properties[comp]['versions'][ver]['mpi']}")
                    mpidict = self.machine_properties[comp]["versions"][ver]["mpi"]
                    mpitypes = mpidict.keys()
                    print(self.machine_properties[comp]["versions"][ver])
                    for key in mpitypes:

                        if "build_time" in self.machine_properties[comp]:
                            self.build_time = self.machine_properties[comp]["build_time"]

                        if "test_time" in self.machine_properties[comp]:
                            self.test_time = self.machine_properties[comp]["test_time"]

                        for branch in self.machine_properties["branch"]:
                            nuopcbranch = branch
                            if "nuopcbranch" in self.machine_properties:
                                nuopcbranch = self.machine_properties["nuopcbranch"]

                            subdir = f"{comp}_{ver}_{key}_{build_type}_{branch}"
                            subdir = re.sub(
                                "/", "_", subdir
                            )  # Some branches have a slash, so replace that with underscore

                            update_repo(subdir, branch, nuopcbranch)

                            self.scheduler.createHeaders(self)
                            self.create_scripts(build_type, comp, ver, mpidict, key)
                            self.scheduler.submitJob(self, subdir, self._mpiver, branch)
                            os.chdir("..")

    def _reclone(self):
        print("recloning")
        rmdir(self._data.artifacts_root)
        os.system("git clone https://github.com/esmf-org/esmf-test-artifacts.git")
        os.chdir("esmf-test-artifacts")
        os.system(f"git checkout -b {self.machine_properties.machine_name}")
        os.chdir("..")

    def _traverse(self):
        for comp in self.machine_properties["compiler"]:
            for ver in self.machine_properties[comp]["versions"]:
                print(self.machine_properties[comp]["versions"][ver])

    def _write_python_test(self, file_out):
        cmds = [
            "cd ../src/addon/ESMPy",
            "export PATH=$PATH:$HOME/.local/bin",
            "python3 setup.py build 2>&1 | tee python_build.log",
            f"ssh {self.machine_properties.headnodename} {os.getcwd()}/runpython.sh 2>&1 | tee python_build.log",
            "python3 setup.py test 2>&1 | tee python_test.log",
            "python3 setup.py test_examples 2>&1 | tee python_examples.log",
            "python3 setup.py test_regrid_from_file 2>&1 | tee python_regrid.log",
        ]
        file_out.writelines(cmds)


def fetch_yaml_properties(*, global_yaml_config_path, local_yaml_config_path) -> Tuple:
    with open(global_yaml_config_path) as file:
        global_properties = GlobalProperties(**yaml.load(file, Loader=yaml.SafeLoader))
    with open(local_yaml_config_path) as file:

        machine_properties = MachineProperties(**yaml.load(file, Loader=yaml.SafeLoader))
    return global_properties, machine_properties


def update_repo(subdir, branch, nuopcbranch, is_dryrun=False):
    os.system(f"rm -rf {subdir}")
    if not os.path.isdir(subdir):

        cmdstring = f"git clone -b {branch} git@github.com:esmf-org/esmf {subdir}"
        nuopcclone = f"git clone -b {nuopcbranch} git@github.com:esmf-org/nuopc-app-prototypes"
        if is_dryrun is True:
            print(f"would have executed {cmdstring}")
            print(f"would have executed {nuopcclone}")
            print(f"would have cd'd to {subdir}")
            return

        status = []
        status.append(subprocess.check_output(cmdstring, shell=True).strip().decode("utf-8"))

        # TODO create directory if doesnt exist using native
        os.chdir(subdir)
        _runcmd("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
        _runcmd(f"git checkout {branch}")
        _runcmd(f"git pull origin {branch}")
        status.append(subprocess.check_output(nuopcclone, shell=True).strip().decode("utf-8"))

        print(f"status from nuopc clone command {nuopcclone} was {status}")


def _write_mpi_environment_variables(mpidict, file_handle):
    if "mpi_env_vars" in mpidict:
        for mpi_var in mpidict["mpi_env_vars"]:
            file_handle.write(f"export {mpidict['mpi_env_vars'][mpi_var]}\n")


def _write_netcdf(netcdf, file_handle, mpi_module_flavor):
    if netcdf == "None":
        file_handle.write(f"module load {netcdf} {mpi_module_flavor} \n\n")
    else:
        file_handle.write(f"module load {netcdf} {mpi_module_flavor} {netcdf}\n")


def _write_extra_env_vars(compiler_version, file_handle):
    if "extra_env_vars" in compiler_version:
        for var in compiler_version["extra_env_vars"]:
            file_handle.write(f"export {var}\n")


def _write_extra_commands(compiler_version, file_handle):
    if "extra_commands" in compiler_version:
        for cmd in compiler_version["extra_commands"]:
            file_handle.write(f"{cmd}\n")


def _runcmd(cmd, is_dryrun=False):
    if is_dryrun is True:
        print(f"would have executed {cmd}")
    else:
        print(f"running {cmd}\n")
        os.system(cmd)


def _filename(run_type, comp, ver, key, build_type):
    return f"{run_type}-{comp}_{ver}_{key}_{build_type}.bat"


def _write_vars(file_out, comp, key, build_type):
    cmds = [
        f"export ESMF_DIR={os.getcwd()}\n"
        f"export ESMF_COMPILER={comp}\n"
        f"export ESMF_COMM={key}\n"
        f"export ESMF_BOPT='{build_type}'\n"
        "export ESMF_TESTEXHAUSTIVE='ON'\n"
        "export ESMF_TESTWITHTHREADS='ON'\n"
    ]
    file_out.writelines(cmds)


def _get_header_list(mpi_flavor):
    _header_list = ["build", "test"]
    if "pythontest" in mpi_flavor:
        _header_list.append("python")
    return _header_list


def get_args():
    """get_args

    Returns:
        list:
    """
    parser = argparse.ArgumentParser(description="Archive collector for ESMF testing framework")
    parser.add_argument(
        "-w",
        "--workdir",
        help="directory where builds will be mad #",
        required=False,
        default=os.getcwd(),
    )
    parser.add_argument(
        "-y",
        "--yaml",
        help="Yaml file defining builds and testing parameters",
        required=True,
    )
    parser.add_argument(
        "-a",
        "--artifacts",
        help="directory where artifacts will be placed",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--dryrun",
        help="directory where artifacts will be placed",
        required=False,
        default=False,
    )
    return vars(parser.parse_args())


if __name__ == "__main__":
    args = get_args()

    _data = ESMFTestData(args["yaml"], args["artifacts"], args["workdir"], args["dryrun"])
    test = ESMFTest(_data)
    test.create_job_card_and_submit()
