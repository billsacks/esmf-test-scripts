import argparse
import glob
import os
import pathlib
import re
import subprocess
import time
from collections import namedtuple
from datetime import datetime

from schedulers.noscheduler import NoScheduler
from schedulers.pbs import PBS
from schedulers.scheduler import Scheduler
from schedulers.slurm import Slurm

ArchiveResultsData = namedtuple(
    "ArchivedResultsData",
    [
        "jobid",
        "build_basename",
        "machine_name",
        "scheduler",
        "test_root_dir",
        "artifacts_root",
        "mpiversion",
        "branch",
        "dryrun",
        "root_path",
    ],
)


class ArchiveResults:
    def __init__(self, _data: ArchiveResultsData):

        self.data = _data
        self._build_time = None
        self._scheduler = None
        self._build_dir = None
        self._build_hash = None
        self._outpath = None

    @property
    def scheduler(self):
        if not self._scheduler:
            self._scheduler = get_scheduler(self.data.scheduler)
        return self._scheduler

    @property
    def build_dir(self):
        if not self._build_dir:
            self._build_dir = f"{self.data.test_root_dir}/{self.data.build_basename}"
        return self._build_dir

    def monitor(self):
        print(f"dryrun is {self.data.dryrun}")
        start_time = time.time()
        seconds = 144000
        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            job_done = self.scheduler.checkqueue(self.data.jobid)
            if job_done:
                oe_filelist = glob.glob(
                    f"{self.data.test_root_dir}/{self.data.build_basename}/*_{self.data.jobid}*.log"
                )
                oe_filelist.extend(
                    glob.glob(
                        f"{self.data.test_root_dir}/{self.data.build_basename}/*.bat"
                    )
                )
                oe_filelist.extend(
                    glob.glob(
                        f"{self.data.test_root_dir}/{self.data.build_basename}/module-*.log".format()
                    )
                )
                print(f"filelist is {oe_filelist}")
                print(f"oe list is {oe_filelist}\n")
                self.copy_artifacts(oe_filelist)
                break
            time.sleep(30)

            if elapsed_time > seconds:
                print("Finished iterating in: " + str(int(elapsed_time)) + " seconds")
                break

    def runcmd(self, cmd):
        if self.data.dryrun is True:
            print(f"would have executed {cmd}")
        else:
            os.system(cmd)

    def create_summary(
        self,
        unit_results,
        system_results,
        example_results,
        nuopc_pass,
        nuopc_fail,
        make_info,
        esmfmkfile,
    ):
        results = (
            subprocess.check_output(
                f"grep ESMF_OS: {self.build_dir}/*_{self.data.jobid}.log",
                shell=True,
            )
            .strip()
            .decode("utf-8")
        )
        esmf_os = results.split()[1]
        print(f"HEY!!! esmf_os is {esmf_os}")

        self._build_time = datetime.now().strftime("%H:%M:%S")
        if len(esmfmkfile) > 0:
            self._build_time = datetime.fromtimestamp(os.path.getmtime(esmfmkfile[0]))

        summary_file = open(f"{self._outpath}/summary.dat", "w")
        summary_file.write(
            "\n===================================================================\n"
        )
        summary_file.write(
            f"Build for = {self.data.build_basename}, mpi version {self.data.mpiversion} on {self.data.machine_name} esmf_os: {esmf_os}\n"
        )
        summary_file.write(f"Build time = {self._build_time}\n")
        summary_file.write(f"git hash = {self.build_hash}\n\n")
        unit_results = re.sub(" FAIL", "\tFAIL", unit_results)
        system_results = re.sub(" FAIL", " \tFAIL", system_results)
        example_results = re.sub(" FAIL", " \tFAIL", example_results)
        summary_file.write(f"unit test results   \t{unit_results}\n")
        summary_file.write(f"system test results \t{system_results}\n")
        summary_file.write(f"example test results \t{example_results}\n")
        summary_file.write(
            f"nuopc test results \tPASS {nuopc_pass} \tFAIL {nuopc_fail}\n\n"
        )
        summary_file.write(
            "\n===================================================================\n"
        )
        summary_file.write(f"\n\n{make_info}\n\n")
        summary_file.write(
            "\n===================================================================\n"
        )
        summary_file.close()

    @property
    def build_hash(self):
        if not self._build_hash:
            self._build_hash = (
                subprocess.check_output("git describe --tags --abbrev=7", shell=True)
                .strip()
                .decode("utf-8")
            )
        return self._build_hash

    def copy_artifacts(self, oe_filelist):

        build_basename = os.path.basename(self.build_dir)
        dirbranch = re.sub("/", "_", self.data.branch)
        cwd = os.getcwd()
        os.chdir(self.build_dir)
        os.chdir(cwd)
        print(f"build_basename is {build_basename}")
        parts = build_basename.split("_")
        # [compiler, version, mpiflavor, build_type,dirbranch] = build_basename.split("_")
        compiler = parts[0]
        version = parts[1]
        mpiflavor = parts[2]
        build_type = parts[3]
        # get the full path for placment of artifacts
        outpath = f"{self.data.artifacts_root}/{dirbranch}/{self.data.machine_name}/{compiler}/{version}/{build_type}/{mpiflavor}"
        if self.data.mpiversion != "None":
            outpath = outpath + f"/{self.data.mpiversion}"
        self._outpath = outpath
        # copy/rename the stdout/stderr files to artifacts out directory
        test_stage = False
        print(f"outpath is {outpath}")
        for cfile in oe_filelist:
            print(f"cfile is {cfile}")
            if int(self.data.jobid) < 0:
                test_stage = True
            if (
                cfile.find(f"test_{self.data.jobid}") != -1
            ):  # this is just the build job, so no test artifacts yet
                test_stage = True
        if not test_stage:
            # remove old files in out directory
            print("just the build stage, so remove old files")
            cmd = f"mkdir -p {outpath}/out; rm {outpath}/*/*; rm {outpath}/*.log; rm {outpath}/summary.dat"
            print(f"cmd is {cmd}\n")
            self.runcmd(cmd)
        # print("oe filelist is {}".format(oe_filelist))
        if oe_filelist == []:
            return
        for cfile in oe_filelist:
            nfile = os.path.basename(re.sub(f"_{self.data.jobid}", "", cfile))
            cp_cmd = f"echo `date` > {outpath}/out/{nfile}"
            self.runcmd(cp_cmd)
            cp_cmd = f"cat {cfile} >> {outpath}/out/{nfile}"
            self.runcmd(cp_cmd)
        if not test_stage:
            command = f"grep success {self.build_dir}/build_{self.data.jobid}.log"
            unit_results = "-1 -1"
            system_results = "-1 -1"
            example_results = "-1 -1"
            nuopc_pass = "-1"
            nuopc_fail = "-1"
            try:
                (
                    subprocess.check_output(f"{command}", shell=True)
                    .strip()
                    .decode("utf-8")
                )
            except subprocess.CalledProcessError:

                example_results = "Build did not complete successfully"
                unit_results = "Build did not complete successfully"
                system_results = "Build did not complete successfully"
                nuopc_pass = "Build did not complete successfully"
                nuopc_fail = "Build did not complete successfully"
            try:
                make_info = (
                    subprocess.check_output(
                        "cat {}/module-build.log; cat {}/info.log".format(
                            self.build_dir, self.build_dir
                        ),
                        shell=True,
                    )
                    .strip()
                    .decode("utf-8")
                )
            except subprocess.CalledProcessError:
                make_info = f"error finding {self.build_dir}/module-build.log or {self.build_dir}/info.log"
            esmfmkfile = glob.glob(f"{self.build_dir}/lib/lib{build_type}/*/esmf.mk")
            self.create_summary(
                unit_results,
                system_results,
                example_results,
                nuopc_pass,
                nuopc_fail,
                make_info,
                esmfmkfile,
            )
            git_cmd = f"cd {self.data.artifacts_root};git checkout {self.data.machine_name};git add {dirbranch}/{self.data.machine_name};git commit -a -m'update for build of {build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]';git push origin {self.data.machine_name}"
            print(f"git_cmd is {git_cmd}")
            self.runcmd(git_cmd)
            return
        # Make directories, if they aren't already there
        cmd = f"mkdir -p {outpath}/examples; rm {outpath}/examples/*; rm {outpath}/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {outpath}/apps; rm {outpath}/apps/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {outpath}/test; rm {outpath}/test/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {outpath}/lib; rm {outpath}/lib/*"
        self.runcmd(cmd)
        print("globbing examples")

        example_artifacts = glob.glob(
            f"{self.build_dir}/examples/examples{build_type}/*/*.Log"
        )
        example_artifacts.extend(
            glob.glob(f"{self.build_dir}/examples/examples{build_type}/*/*.stdout")
        )
        # get information from example results file to accumulate
        ex_result_file = glob.glob(
            f"{self.build_dir}/examples/examples{build_type}/*/*results"
        )
        if len(ex_result_file) > 0:
            example_results = (
                subprocess.check_output(f"cat {ex_result_file[0]}", shell=True)
                .strip()
                .decode("utf-8")
            )
        else:
            example_results = "No examples ran"
        # get information from test results files to accumulate
        test_artifacts = glob.glob(f"{self.build_dir}/test/test{build_type}/*/*.Log")
        print("test_artifacts are ", test_artifacts)
        test_artifacts.extend(
            glob.glob(f"{self.build_dir}/test/test{build_type}/*/*.stdout")
        )
        try:
            unit_results = (
                subprocess.check_output(
                    f"cat {self.build_dir}/test/test{build_type}/*/unit_tests_results",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError:
            unit_results = "unit tests did not complete"
        try:
            system_results = (
                subprocess.check_output(
                    "cat {self.build_dir}/test/test{build_type}/*/system_tests_results",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError:
            system_results = "system tests did not complete"
        try:
            nuopc_pass = (
                subprocess.check_output(
                    f"grep PASS: {self.build_dir}/nuopc_{self.data.jobid}.log | wc -l",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
            nuopc_fail = (
                subprocess.check_output(
                    f"grep FAIL: {self.build_dir}/nuopc_{self.data.jobid}.log | wc -l",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError:
            nuopc_pass = 0
            nuopc_fail = 0
        python_artifacts = glob.glob(f"{self.build_dir}/src/addon/ESMPy/*.log")

        cwd = os.getcwd()
        os.chdir(self.build_dir)
        make_info = (
            subprocess.check_output("cat module-build.log; cat info.log", shell=True)
            .strip()
            .decode("utf-8")
        )
        os.chdir(cwd)
        esmfmkfile = glob.glob(f"{self.build_dir}/lib/lib{build_type}/*/esmf.mk")
        print(f"esmfmkfile is {esmfmkfile}")
        self.create_summary(
            unit_results,
            system_results,
            example_results,
            nuopc_pass,
            nuopc_fail,
            make_info,
            esmfmkfile,
        )
        timestamp = f"build time -- {self._build_time}"
        for afile in example_artifacts:
            cmd = f"echo {timestamp} > {outpath}/examples/{os.path.basename(afile)}"
            self.runcmd(cmd)
            cmd = f"cat {afile} >> {outpath}/examples/{os.path.basename(afile)}"
            #   cmd = 'cp {} {}/examples'.format(afile,outpath)
            print(f"cmd is {cmd}")
            self.runcmd(cmd)
        for afile in test_artifacts:
            cmd = f"echo {timestamp} > {outpath}/test/{os.path.basename(afile)}"
            self.runcmd(cmd)
            cmd = f"cat {afile} >> {outpath}/test/{os.path.basename(afile)}"
            #   cmd = 'cp {} {}/test".format(afile,outpath)
            print(f"cmd is {cmd}")
            self.runcmd(cmd)
        for afile in esmfmkfile:
            cmd = f"echo {timestamp} > {outpath}/lib/{os.path.basename(afile)}"
            self.runcmd(cmd)
            cmd = f"cat {afile} >> {outpath}/lib/{os.path.basename(afile)}"
            #   cmd = 'cp {} {}/lib'.format(afile,outpath)
            print(f"cmd is {cmd}")
            self.runcmd(cmd)
        for afile in python_artifacts:
            cmd = f"echo {timestamp} > {outpath}/{os.path.basename(afile)}"
            self.runcmd(cmd)
            cmd = f"cat {afile} >> {outpath}/{os.path.basename(afile)}"
            #   cmd = 'cp {} {}'.format(afile,outpath)
            print(f"cmd is {cmd}")
            self.runcmd(cmd)

        git_cmd = f"cd {self.data.artifacts_root};git checkout {self.data.machine_name};git add {dirbranch}/{self.data.machine_name};git commit -a -m'update for test of {build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]';git push origin {self.data.machine_name}"
        self.runcmd(git_cmd)
        return


def get_scheduler(scheduler: Scheduler) -> Scheduler:
    if scheduler == "pbs":
        return PBS()
    elif scheduler == "slurm":
        return Slurm()
    return NoScheduler()


if __name__ == "__main__":
    # monitor_cmd_build = "python3 {}/archive_results.py -j {} -b {} -m {} -s {} -t {} -a {} -M {} -B {} -d {}".format(
    #           test.mypath,
    #           jobnum,
    #           subdir,
    #           test.machine_name,
    #           self.type,
    #           test.script_dir,
    #           test.artifacts_root,
    #           mpiver,
    #           branch,
    #           test.dryrun,
    #       )
    parser = argparse.ArgumentParser(description="ESMF nightly build/test system")
    parser.add_argument(
        "-j", "--self.jobid", help="directory where builds will be mad #", required=True
    )
    parser.add_argument(
        "-b",
        "--buildbasename",
        help="directory where artifacts will be collected",
        required=True,
    )
    parser.add_argument(
        "-m",
        "--machinename",
        help="name of machine where tests were run",
        required=False,
        default=False,
    )
    parser.add_argument(
        "-s", "--scheduler", help="type of scheduler used", required=False, default=None
    )
    parser.add_argument(
        "-t",
        "--testrootdir",
        help="root directory containing python_scritps",
        required=True,
    )
    parser.add_argument(
        "-a",
        "--artifactsrootdir",
        help="directory where artifacts will be placed",
        required=True,
    )
    parser.add_argument("-M", "--mpiversion", help="mpi version used", required=True)
    parser.add_argument("-B", "--branch", help="branch tested", required=True)
    parser.add_argument("-d", "--dryrun", help="dryrun?", required=False, default=False)
    args = vars(parser.parse_args())

    data = ArchiveResultsData(
        root_path=pathlib.Path(__file__).parent.absolute(),
        jobid=args["jobid"],
        build_basename=args["buildbasename"],
        machine_name=args["machinename"],
        scheduler=args["scheduler"],
        test_root_dir=args["testrootdir"],
        artifacts_root=args["artifactsrootdir"],
        mpiversion=args["mpiversion"],
        branch=args["branch"],
        dryrun=args["dryrun"],
    )

    archiver = ArchiveResults(data)
