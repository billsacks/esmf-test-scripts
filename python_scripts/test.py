# pylint: disable=unspecified-encoding

import os
import re
from collections import namedtuple
from typing import List

from schedulers.noscheduler import NoScheduler
from schedulers.pbs import pbs
from schedulers.scheduler import Scheduler
from schedulers.slurm import slurm
from shared import update_repo

ESMFTestUserInput = namedtuple(
    "ESMFTestUserInput", ["yaml_file", "artifacts_root", "workdir", "dryrun"]
)

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


def parse_test_data(test):
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


class ESMFTest:
    build_types = ["O", "g"]

    def __init__(self, data: ESMFTestUserInput, machine_properties):

        self._data = data
        self._scheduler = None
        self._mpiver = None

        self.machine_properties = machine_properties

    @property
    def scheduler(self) -> Scheduler:
        _map = {"slurm": slurm, "pbs": pbs, "None": NoScheduler}
        if self._scheduler is None:
            self._scheduler = _map[self.machine_properties.scheduler_type]()
        return self._scheduler

    @property
    def artifacts_root(self) -> str:
        return self._data.artifacts_root

    @property
    def script_dir(self) -> str:
        return os.getcwd()

    @property
    def machine_name(self) -> str:
        return self.machine_properties.machine_name

    @property
    def do_reclone(self) -> bool:
        return self.machine_properties["reclone-artifacts"]

    @property
    def dryrun(self) -> bool:
        if self._data.dryrun:
            return True
        return False

    @property
    def https(self) -> bool:
        return "git-https" in self.machine_properties

    @property
    def python_script_header(self) -> List[str]:
        return [
            f"#!{self.machine_properties.bash} -l\n",
            f"cd {os.getcwd()}\n",
            "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n",
            f"cd {os.getcwd()}/src/addon/ESMPy\n",
        ]

    def get_time_by_type(self, comp, _typetype: str) -> str:
        if f"{_typetype}_time" in self.machine_properties[comp]:
            return self.machine_properties[comp][f"{_typetype}_time"]
        return "1:00:00"

    def create_scripts(self, build_type, comp, ver, mpidict, key) -> None:
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
                    file_out.write(
                        f"\nmodule use {self.machine_properties['modulepath']}\n"
                    )
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
                _write_vars(
                    file_out=file_out, build_type=build_type, comp=comp, key=key
                )

                if header_type == "build":

                    file_out.write(
                        f"make -j {self.machine_properties.cpn} 2>&1| tee build_$JOBID.log\n\n"
                    )
                elif header_type == "test":
                    file_out.writelines(
                        [
                            "make info 2>&1| tee info.log",
                            "make install 2>&1| tee install_$JOBID.log",
                            "make all_tests 2>&1| tee test_$JOBID.log",
                            "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`",
                        ]
                    )
                    if mpiflavor["module"] != "None":
                        file_out.writelines(
                            [
                                "chmod +x runpython.sh",
                                "cd nuopc-app-prototypes",
                                "./testProtos.sh 2>&1| tee ../nuopc_$JOBID.log",
                            ]
                        )
                else:
                    file_out.writelines(
                        [
                            "python3 setup.py test_examples_dryrun",
                            "python3 setup.py test_regrid_from_file_dryrun",
                        ]
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

                        for branch in self.machine_properties["branch"]:
                            nuopcbranch = branch
                            if "nuopcbranch" in self.machine_properties:
                                nuopcbranch = self.machine_properties["nuopcbranch"]

                            subdir = f"{comp}_{ver}_{key}_{build_type}_{branch}"
                            subdir = re.sub(
                                "/", "_", subdir
                            )  # Some branches have a slash, so replace that with underscore

                            update_repo(subdir, branch, nuopcbranch)

                            self.scheduler.create_headers(parse_test_data(self))
                            self.create_scripts(build_type, comp, ver, mpidict, key)
                            self.scheduler.submit_job(
                                parse_test_data(self), subdir, self._mpiver, branch
                            )
                            os.chdir("..")

    def _traverse(self):
        """Not sure if needed"""
        # TODO Deprecate
        for comp in self.machine_properties["compiler"]:
            for ver in self.machine_properties[comp]["versions"]:
                print(self.machine_properties[comp]["versions"][ver])

    def _write_python_test(self, file_out):
        file_out.writelines(
            [
                "cd ../src/addon/ESMPy",
                "export PATH=$PATH:$HOME/.local/bin",
                "python3 setup.py build 2>&1 | tee python_build.log",
                f"ssh {self.machine_properties.headnodename} {os.getcwd()}/runpython.sh 2>&1 | tee python_build.log",
                "python3 setup.py test 2>&1 | tee python_test.log",
                "python3 setup.py test_examples 2>&1 | tee python_examples.log",
                "python3 setup.py test_regrid_from_file 2>&1 | tee python_regrid.log",
            ]
        )


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
