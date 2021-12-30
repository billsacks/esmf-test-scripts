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


ESMFTestData = namedtuple(
    "ESMFTestData", ["yaml_file", "artifacts_root", "workdir", "dryrun"]
)


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
        self._data = data
        self._scheduler = None

        print("calling readyaml")
        self.global_properties, self.machine_properties = self._readYAML(
            local_yaml_config_path=data.yaml_file,
            global_yaml_config_path=self.global_yaml_config_path,
        )

        print(
            self._data.yaml_file,
            self._data.artifacts_root,
            self._data.workdir,
        )
        self.createJobCardsAndSubmit()

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

    def _reclone(self):
        print("recloning")
        rmdir(self._data.artifacts_root)
        os.system("git clone https://github.com/esmf-org/esmf-test-artifacts.git")
        os.chdir("esmf-test-artifacts")
        os.system(f"git checkout -b {self.machine_properties.machine_name}")
        os.chdir("..")

    def _readYAML(self, *, global_yaml_config_path, local_yaml_config_path) -> Tuple:
        with open(global_yaml_config_path) as file:
            global_properties = GlobalProperties(
                **yaml.load(file, Loader=yaml.SafeLoader)
            )
        with open(local_yaml_config_path) as file:

            machine_properties = MachineProperties(
                **yaml.load(file, Loader=yaml.SafeLoader)
            )
        return global_properties, machine_properties

    def _traverse(self):
        for comp in self.machine_properties["compiler"]:
            for ver in self.machine_properties[comp]["versions"]:
                print(self.machine_properties[comp]["versions"][ver])

    def runcmd(self, cmd):
        if self.dryrun == True:
            print(f"would have executed {cmd}")
        else:
            print(f"running {cmd}\n")
            os.system(cmd)

    def createScripts(self, build_type, comp, ver, mpidict, key):
        mpiflavor = mpidict[key]
        if "pythontest" in mpiflavor:
            headerList = ["build", "test", "python"]
        else:
            headerList = ["build", "test"]
        for headerType in headerList:
            if headerType == "build":
                file_out = self.fb
            elif headerType == "test":
                file_out = self.ft
            else:
                pythonscript = open("runpython.sh", "w")
                file_out = pythonscript
                file_out.write(f"#!{self.machine_properties.bash} -l\n")
                file_out.write(f"cd {os.getcwd()}\n")
                file_out.write(
                    "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n"
                )
                file_out.write(f"cd {os.getcwd()}/src/addon/ESMPy\n")
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
                cmdstring = "export ESMF_MPIRUN={}/src/Infrastructure/stubs/mpiuni/mpirun\n".format(
                    os.getcwd()
                )
                file_out.write(cmdstring)

            if "mpi_env_vars" in mpidict[key]:
                for mpi_var in mpidict[key]["mpi_env_vars"]:
                    file_out.write(f"export {mpidict[key]['mpi_env_vars'][mpi_var]}\n")

            if self.machine_properties[comp]["versions"][ver]["netcdf"] == "None":
                modulecmd = "module load {} {} \n\n".format(
                    self.machine_properties[comp]["versions"][ver]["compiler"],
                    mpiflavor["module"],
                )
                esmfnetcdf = "\n"
                file_out.write(modulecmd)
            else:
                modulecmd = "module load {} {} {}\n".format(
                    self.machine_properties[comp]["versions"][ver]["compiler"],
                    mpiflavor["module"],
                    self.machine_properties[comp]["versions"][ver]["netcdf"],
                )
                esmfnetcdf = "export ESMF_NETCDF=nc-config\n\n"
                file_out.write(modulecmd)

            if "hdf5" in self.machine_properties[comp]["versions"][ver]:
                modulecmd = "module load {} \n".format(
                    self.machine_properties[comp]["versions"][ver]["hdf5"]
                )
                file_out.write(modulecmd)
            if "netcdf-fortran" in self.machine_properties[comp]["versions"][ver]:
                modulecmd = "module load {} \n".format(
                    self.machine_properties[comp]["versions"][ver]["netcdf-fortran"]
                )
                file_out.write(modulecmd)

            if headerType == "build":
                file_out.write("module list >& module-build.log\n\n")
            else:
                file_out.write("module list >& module-test.log\n\n")

            file_out.write("set -x\n")
            file_out.write(esmfnetcdf)

            if "extra_env_vars" in self.machine_properties[comp]["versions"][ver]:
                for var in self.machine_properties[comp]["versions"][ver][
                    "extra_env_vars"
                ]:
                    file_out.write(
                        "export {}\n".format(
                            self.machine_properties[comp]["versions"][ver][
                                "extra_env_vars"
                            ][var]
                        )
                    )

            if "extra_commands" in self.machine_properties[comp]["versions"][ver]:
                for cmd in self.machine_properties[comp]["versions"][ver][
                    "extra_commands"
                ]:
                    file_out.write(
                        "{}\n".format(
                            self.machine_properties[comp]["versions"][ver][
                                "extra_commands"
                            ][cmd]
                        )
                    )

            self.write_vars(
                file_out=file_out, build_type=build_type, comp=comp, key=key
            )

            if headerType == "build":
                #       cmdstring = "make -j {} clean 2>&1| tee clean_$JOBID.log \nmake -j {} 2>&1| tee build_$JOBID.log\n\n".format(self.cpn,self.cpn)
                cmdstring = f"make -j {self.machine_properties.cpn} 2>&1| tee build_$JOBID.log\n\n"
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
                self.write_python_test(file_out)

            file_out.close()
            mpimodule = mpiflavor["module"]
            if mpimodule == "":
                self.mpiver = "None"
            else:
                self.mpiver = mpiflavor["module"].split("/")[-1]

    def write_python_test(self, file_out):
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

    def write_vars(self, file_out, comp, key, build_type):
        cmds = [
            f"export ESMF_DIR={os.getcwd()}\n"
            f"export ESMF_COMPILER={comp}\n"
            f"export ESMF_COMM={key}\n"
            f"export ESMF_BOPT='{build_type}'\n"
            "export ESMF_TESTEXHAUSTIVE='ON'\n"
            "export ESMF_TESTWITHTHREADS='ON'\n"
        ]
        file_out.writelines(cmds)

    def createJobCardsAndSubmit(self):
        for build_type in self.build_types:
            for comp in self.machine_properties["compiler"]:
                for ver in self.machine_properties[comp]["versions"]:
                    print(f"{self.machine_properties[comp]['versions'][ver]['mpi']}")
                    mpidict = self.machine_properties[comp]["versions"][ver]["mpi"]
                    mpitypes = mpidict.keys()
                    print(self.machine_properties[comp]["versions"][ver])
                    for key in mpitypes:
                        if "build_time" in self.machine_properties[comp]:
                            self.build_time = self.machine_properties[comp][
                                "build_time"
                            ]
                        else:
                            self.build_time = "1:00:00"
                        if "test_time" in self.machine_properties[comp]:
                            self.test_time = self.machine_properties[comp]["test_time"]
                        else:
                            self.test_time = "1:00:00"
                        for branch in self.machine_properties["branch"]:
                            if "nuopcbranch" in self.machine_properties:
                                nuopcbranch = self.machine_properties["nuopcbranch"]
                            else:
                                nuopcbranch = branch
                            subdir = f"{comp}_{ver}_{key}_{build_type}_{branch}"
                            subdir = re.sub(
                                "/", "_", subdir
                            )  # Some branches have a slash, so replace that with underscore

                            updateRepo(subdir, branch, nuopcbranch)
                            self.b_filename = "build-{}_{}_{}_{}.bat".format(
                                comp, ver, key, build_type
                            )
                            self.t_filename = "test-{}_{}_{}_{}.bat".format(
                                comp, ver, key, build_type
                            )
                            self.fb = open(self.b_filename, "w")
                            self.ft = open(self.t_filename, "w")
                            self.scheduler.createHeaders(self)
                            self.createScripts(build_type, comp, ver, mpidict, key)
                            self.scheduler.submitJob(self, subdir, self.mpiver, branch)
                            os.chdir("..")


def updateRepo(subdir, branch, nuopcbranch, is_dryrun=False):
    os.system(f"rm -rf {subdir}")
    if not (os.path.isdir(subdir)):

        cmdstring = f"git clone -b {branch} git@github.com:esmf-org/esmf {subdir}"
        nuopcclone = (
            f"git clone -b {nuopcbranch} git@github.com:esmf-org/nuopc-app-prototypes"
        )
        if is_dryrun is True:
            print(f"would have executed {cmdstring}")
            print(f"would have executed {nuopcclone}")
            print(f"would have cd'd to {subdir}")

        else:
            status = []
            status.append(
                subprocess.check_output(cmdstring, shell=True).strip().decode("utf-8")
            )

            # TODO create directory if doesnt exist using native
            os.chdir(subdir)
            _runcmd("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
            _runcmd(f"git checkout {branch}")
            _runcmd(f"git pull origin {branch}")
            status.append(
                subprocess.check_output(nuopcclone, shell=True).strip().decode("utf-8")
            )

            print(f"status from nuopc clone command {nuopcclone} was {status}")


def _runcmd(cmd, is_dryrun=False):
    if is_dryrun is True:
        print(f"would have executed {cmd}")
    else:
        print(f"running {cmd}\n")
        os.system(cmd)


def get_args():
    parser = argparse.ArgumentParser(
        description="Archive collector for ESMF testing framework"
    )
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

    _data = ESMFTestData(
        args["yaml"], args["artifacts"], args["workdir"], args["dryrun"]
    )
    test = ESMFTest(_data)
