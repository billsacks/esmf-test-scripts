import yaml
import os
import re
import time
import subprocess
import sys
import pathlib
import argparse
from schedulers.scheduler import scheduler
from schedulers.noscheduler import NoScheduler
from schedulers.pbs import pbs
from schedulers.slurm import slurm
import shutil


def rmdir(path):
    shutil.rmtree(path)


class ESMFTest:
    def __init__(self, yaml_file, artifacts_root, workdir, dryrun):
        self.yaml_file = yaml_file
        self.artifacts_root = artifacts_root
        self.workdir = workdir
        if dryrun == "True":
            self.dryrun = True
        else:
            self.dryrun = False
        print(f"setting dryrun to {self.dryrun}")
        self.mypath = pathlib.Path(__file__).parent.absolute()
        print(f"path is {self.mypath}")
        print("calling readyaml")
        self.readYAML()
        if self.reclone == True:
            print("recloning")
            rmdir(self.artifacts_root)
            os.system("git clone https://github.com/esmf-org/esmf-test-artifacts.git")
            os.chdir("esmf-test-artifacts")
            os.system(f"git checkout -b {self.machine_name}")
            os.chdir("..")
        if self.scheduler_type == "slurm":
            self.scheduler = slurm("slurm")
        elif self.scheduler_type == "None":
            self.scheduler = NoScheduler("None")
        elif self.scheduler_type == "pbs":
            self.scheduler = pbs("pbs")
        print(self.yaml_file, self.artifacts_root, self.workdir)
        self.createJobCardsAndSubmit()

    def readYAML(self):
        config_path = os.path.dirname(self.yaml_file)
        global_file = os.path.join(config_path, "global.yaml")
        print(f"HEY!!!! {global_file}")
        with open(global_file) as file:
            self.global_list = yaml.load(file, Loader=yaml.SafeLoader)
            if "reclone-artifacts" in self.global_list:
                self.reclone = self.global_list["reclone-artifacts"]
            else:
                self.reclone = False
            print(f"set reclone to {self.reclone}")
        with open(self.yaml_file) as file:
            self.machine_list = yaml.load(file, Loader=yaml.SafeLoader)
            self.machine_name = self.machine_list["machine"]
            print(f"machine name is {self.machine_name}")
            if "git-https" in self.machine_list:
                self.https = True
            else:
                self.https = False
            if "bash" in self.machine_list:
                self.bash = self.machine_list["bash"]
            else:
                self.bash = "/bin/bash"
            if "account" in self.machine_list:
                self.account = self.machine_list["account"]
            else:
                self.account = "None"
            if "partition" in self.machine_list:
                self.partition = self.machine_list["partition"]
            else:
                self.partition = "None"
            if "queue" in self.machine_list:
                self.queue = self.machine_list["queue"]
            else:
                self.queue = "None"
            if "headnodename" in self.machine_list:
                self.headnodename = self.machine_list["headnodename"]
            else:
                self.headnodename = os.uname()[1]
            #     if("branch" in self.machine_list):
            #       self.branch = self.machine_list['branch']
            #     else:
            #       self.branch = "develop"
            if "nuopcbranch" in self.machine_list:
                self.nuopcbranch = self.machine_list["nuopcbranch"]
            else:
                self.nuopcbranch = "develop"
            self.cpn = self.machine_list["corespernode"]
            self.scheduler_type = self.machine_list["scheduler"]
            self.build_types = ["O", "g"]
            #     self.build_types = ['O']
            self.script_dir = os.getcwd()
            if "cluster" in self.machine_list:
                self.cluster = self.machine_list["cluster"]
            else:
                self.cluster = "None"
            if "constraint" in self.machine_list:
                self.constraint = self.machine_list["constraint"]
            else:
                self.constraint = "None"

            # Now traverse the tree
            for build_type in self.build_types:
                for comp in self.machine_list["compiler"]:

                    for ver in self.machine_list[comp]["versions"]:
                        mpidict = self.machine_list[comp]["versions"][ver]["mpi"]
                        mpitypes = mpidict.keys()
                        print(self.machine_list[comp]["versions"][ver])

    #         for key in mpitypes:
    #           subdir="{}_{}_{}_{}".format(comp,ver,key,build_type)
    #           print("{}".format(subdir))

    def runcmd(self, cmd):
        if self.dryrun == True:
            print(f"would have executed {cmd}")
        else:
            print(f"running {cmd}\n")
            os.system(cmd)

    def updateRepo(self, subdir, branch, nuopcbranch):
        os.system(f"rm -rf {subdir}")
        if not (os.path.isdir(subdir)):
            if self.https == True:
                cmdstring = f"git clone -b {branch} https://github.com/esmf-org/esmf {subdir}"
                nuopcclone = (
                    "git clone -b {} https://github.com/esmf-org/nuopc-app-prototypes".format(
                        nuopcbranch
                    )
                )
            else:
                cmdstring = "git clone -b {} git@github.com:esmf-org/esmf {}".format(branch, subdir)
                nuopcclone = "git clone -b {} git@github.com:esmf-org/nuopc-app-prototypes".format(
                    nuopcbranch
                )
            if self.dryrun == True:
                print(f"would have executed {cmdstring}")
                print(f"would have executed {nuopcclone}")
                print(f"would have cd'd to {subdir}")
                os.system(f"mkdir {subdir}")
                os.chdir(subdir)
            else:
                status = subprocess.check_output(cmdstring, shell=True).strip().decode("utf-8")
                os.chdir(subdir)
                self.runcmd("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
                self.runcmd(f"git checkout {branch}")
                self.runcmd(f"git pull origin {branch}")
                status = subprocess.check_output(nuopcclone, shell=True).strip().decode("utf-8")
                print(f"status from nuopc clone command {nuopcclone} was {status}")

    def createScripts(self, build_type, comp, ver, mpidict, mpitypes, key, branch):
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
                file_out.write(f"#!{self.bash} -l\n")
                file_out.write(f"cd {os.getcwd()}\n")
                file_out.write("export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n")
                file_out.write(f"cd {os.getcwd()}/src/addon/ESMPy\n")
            if "unloadmodule" in self.machine_list[comp]:
                file_out.write(f"\nmodule unload {self.machine_list[comp]['unloadmodule']}\n")
            if "modulepath" in self.machine_list:
                modulepath = self.machine_list["modulepath"]
                file_out.write(f"\nmodule use {self.machine_list['modulepath']}\n")
            if "extramodule" in self.machine_list[comp]:
                file_out.write(f"\nmodule load {self.machine_list[comp]['extramodule']}\n")

            if mpiflavor["module"] == "None":
                mpiflavor["module"] = ""
                cmdstring = "export ESMF_MPIRUN={}/src/Infrastructure/stubs/mpiuni/mpirun\n".format(
                    os.getcwd()
                )
                file_out.write(cmdstring)

            if "mpi_env_vars" in mpidict[key]:
                for mpi_var in mpidict[key]["mpi_env_vars"]:
                    file_out.write(f"export {mpidict[key]['mpi_env_vars'][mpi_var]}\n")

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

            cmdstring = f"export ESMF_DIR={os.getcwd()}\n"
            file_out.write(cmdstring)

            cmdstring = f"export ESMF_COMPILER={comp}\n"
            file_out.write(cmdstring)

            cmdstring = f"export ESMF_COMM={key}\n"
            file_out.write(cmdstring)

            cmdstring = f"export ESMF_BOPT='{build_type}'\n"
            file_out.write(cmdstring)

            cmdstring = "export ESMF_TESTEXHAUSTIVE='ON'\n"
            file_out.write(cmdstring)

            cmdstring = "export ESMF_TESTWITHTHREADS='ON'\n"
            file_out.write(cmdstring)

            if headerType == "build":
                #       cmdstring = "make -j {} clean 2>&1| tee clean_$JOBID.log \nmake -j {} 2>&1| tee build_$JOBID.log\n\n".format(self.cpn,self.cpn)
                cmdstring = f"make -j {self.cpn} 2>&1| tee build_$JOBID.log\n\n"
                file_out.write(cmdstring)
            elif headerType == "test":
                cmdstring = "make info 2>&1| tee info.log \nmake install 2>&1| tee install_$JOBID.log \nmake all_tests 2>&1| tee test_$JOBID.log \n"
                file_out.write(cmdstring)
                #       file_out.write("ssh {} {}/{}/getres-int.sh\n".format(self.headnodename,self.script_dir,os.getcwd()))
                cmdstring = "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n"
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
                cmdstring = (
                    "python3 setup.py test_examples 2>&1 | tee python_examples.log\n".format(
                        self.headnodename
                    )
                )
                file_out.write(cmdstring)
                cmdstring = (
                    "python3 setup.py test_regrid_from_file 2>&1 | tee python_regrid.log\n".format(
                        self.headnodename
                    )
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
        get_res_file.write(f"#!{self.bash} -l\n")
        get_res_file.write(f"{monitor_cmd_build} >& build-res.log &\n")
        get_res_file.close()
        os.system("chmod +x getres-build.sh")

        get_res_file = open("getres-test.sh", "w")
        get_res_file.write(f"#!{self.bash} -l\n")
        get_res_file.write(f"{monitor_cmd_test} >& test-res.log &\n")
        get_res_file.close()
        os.system("chmod +x getres-test.sh")

    def createJobCardsAndSubmit(self):
        for build_type in self.build_types:
            for comp in self.machine_list["compiler"]:
                for ver in self.machine_list[comp]["versions"]:
                    print(f"{self.machine_list[comp]['versions'][ver]['mpi']}")
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
                            subdir = f"{comp}_{ver}_{key}_{build_type}_{branch}"
                            subdir = re.sub(
                                "/", "_", subdir
                            )  # Some branches have a slash, so replace that with underscore
                            if self.https == True:
                                cmdstring = (
                                    "git clone -b {} https://github.com/esmf-org/esmf {}".format(
                                        branch, subdir
                                    )
                                )
                                nuopcclone = "git clone -b {} https://github.com/esmf-org/nuopc-app-prototypes".format(
                                    nuopcbranch
                                )
                            else:
                                cmdstring = (
                                    "git clone -b {} git@github.com:esmf-org/esmf {}".format(
                                        branch, subdir
                                    )
                                )
                                nuopcclone = "git clone -b {} git@github.com:esmf-org/nuopc-app-prototypes".format(
                                    nuopcbranch
                                )
                            self.updateRepo(subdir, branch, nuopcbranch)
                            self.b_filename = "build-{}_{}_{}_{}.bat".format(
                                comp, ver, key, build_type
                            )
                            self.t_filename = "test-{}_{}_{}_{}.bat".format(
                                comp, ver, key, build_type
                            )
                            self.fb = open(self.b_filename, "w")
                            self.ft = open(self.t_filename, "w")
                            self.scheduler.createHeaders(self)
                            self.createScripts(
                                build_type, comp, ver, mpidict, mpitypes, key, branch
                            )
                            self.scheduler.submitJob(self, subdir, self.mpiver, branch)
                            os.chdir("..")


if __name__ == "__main__":
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
    args = vars(parser.parse_args())

    test = ESMFTest(args["yaml"], args["artifacts"], args["workdir"], args["dryrun"])
