import argparse
import glob
import os
import pathlib
import re
import subprocess
import time
from collections import namedtuple
from datetime import datetime
from typing import Any, List, Tuple

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

SummaryData = namedtuple(
    "SummaryData",
    [
        "unit_results",
        "system_results",
        "example_results",
        "nuopc_pass",
        "nuopc_fail",
        "make_info",
        "esmfmkfile",
    ],
)

BuildPermutation = namedtuple(
    "BuildPermutation", ["compiler", "version", "mpiflavor", "build_type"]
)

HORIZONTAL_LINE = f"\n{'=' * 67}\n"
CWD = os.getcwd()


class ArchiveResults:
    def __init__(self, _data: ArchiveResultsData):

        self.data = _data
        self._build_time = None
        self._scheduler = None
        self._build_dir = None
        self._build_hash = None
        self._outpath = None
        self._build_basename = None
        self._dir_branch = None
        self._build_permutation = None
        self._esmf_os = None
        self._esmfmkfile = None

    @property
    def build_time(self):
        if self._build_time is not None:
            return self._build_time
        if self._esmfmkfile is None or len(self._esmfmkfile) < 1:
            _build_time = datetime.now().strftime("%H:%M:%S")
        else:
            _build_time = datetime.fromtimestamp(os.path.getmtime(self._esmfmkfile[0]))

        return _build_time

    @property
    def esmfmkfile(self):
        permutation = self.build_permutation
        if not self._esmfmkfile:
            self._esmfmkfile = glob.glob(
                f"{self.build_dir}/lib/lib{permutation.build_type}/*/esmf.mk"
            )

    @property
    def dirbranch(self):
        if not self._dir_branch:
            self._dir_branch = re.sub("/", "_", self.data.branch)
        return self._dir_branch

    @property
    def build_basename(self):
        if not self._build_basename:
            self._build_basename = os.path.basename(self.build_dir)
        return self._build_basename

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

    @property
    def esmf_os(self):
        if not self._esmf_os:
            results = (
                subprocess.check_output(
                    f"grep ESMF_OS: {self.build_dir}/*_{self.data.jobid}.log",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
            self._esmf_os = results.split()[1]
        return self._esmf_os

    @property
    def build_permutation(self) -> BuildPermutation:
        if not self._build_permutation:
            compiler, version, mpiflavor, build_type = self.build_basename.split("_")
            self._build_permutation = BuildPermutation(compiler, version, mpiflavor, build_type)
        return self._build_permutation

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
                    glob.glob(f"{self.data.test_root_dir}/{self.data.build_basename}/*.bat")
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
        _runcmd(cmd, self.data.dryrun)

    def _get_test_results(self, _data: SummaryData):
        results = []
        unit_results = re.sub(" FAIL", "\tFAIL", _data.unit_results)
        system_results = re.sub(" FAIL", " \tFAIL", _data.system_results)
        example_results = re.sub(" FAIL", " \tFAIL", _data.example_results)
        results.append(f"unit test results   \t{unit_results}\n")
        results.append(f"system test results \t{system_results}\n")
        results.append(f"example test results \t{example_results}\n")
        results.append(
            f"nuopc test results \tPASS {_data.nuopc_pass} \tFAIL {_data.nuopc_fail}\n\n"
        )
        return results

    def create_summary(self, _data: SummaryData):

        print(f"HEY!!! esmf_os is {self.esmf_os}")

        summary_file = open(f"{self.outpath}/summary.dat", "w")
        summary_file.write(HORIZONTAL_LINE)
        summary_file.write(self._get_summary_header())
        summary_file.write(f"Build time = {self.build_time}\n")
        summary_file.write(f"git hash = {self.build_hash}\n\n")
        summary_file.writelines(self._get_test_results(_data))
        summary_file.write(HORIZONTAL_LINE)
        summary_file.write(f"\n\n{_data.make_info}\n\n")
        summary_file.write(HORIZONTAL_LINE)
        summary_file.close()

    def _get_summary_header(
        self,
    ):
        return f"Build for = {self.data.build_basename}, mpi version {self.data.mpiversion} on {self.data.machine_name} esmf_os: {self.esmf_os}\n"

    @property
    def build_hash(self):
        if not self._build_hash:
            self._build_hash = (
                subprocess.check_output("git describe --tags --abbrev=7", shell=True)
                .strip()
                .decode("utf-8")
            )
        return self._build_hash

    @property
    def outpath(self):
        # [compiler, version, mpiflavor, build_type,dirbranch] = self.build_basename.split("_")
        if not self._outpath:
            p = self.build_permutation  # pylint: disable=invalid-name
            result = f"{self.data.artifacts_root}/{self.dirbranch}/{self.data.machine_name}/{p.compiler}/{p.version}/{p.build_type}/{p.mpiflavor}"
            if self.data.mpiversion != "None":
                result = result + f"/{self.data.mpiversion}"
            self._outpath = result
        return self._outpath

    def is_test_stage(self, oe_filelist):
        for cfile in oe_filelist:
            print(f"cfile is {cfile}")
            if int(self.data.jobid) < 0:
                return True
            if (
                cfile.find(f"test_{self.data.jobid}") != -1
            ):  # this is just the build job, so no test artifacts yet
                return True
        return False

    def nuke_old_files(self):
        # remove old files in out directory
        print("just the build stage, so remove old files")
        cmd = f"mkdir -p {self.outpath}/out; rm {self.outpath}/*/*; rm {self.outpath}/*.log; rm {self.outpath}/summary.dat"
        print(f"cmd is {cmd}\n")
        self.runcmd(cmd)

    def generate_new_output_files(self, oe_filelist):
        for cfile in oe_filelist:
            nfile = os.path.basename(re.sub(f"_{self.data.jobid}", "", cfile))
            cp_cmd = f"echo `date` > {self.outpath}/out/{nfile}"
            self.runcmd(cp_cmd)
            cp_cmd = f"cat {cfile} >> {self.outpath}/out/{nfile}"
            self.runcmd(cp_cmd)

    def final_stage(self):
        permutation = self.build_permutation
        command = f"grep success {self.build_dir}/build_{self.data.jobid}.log"
        unit_results = "-1 -1"  # TODO Update this as per discussion early December
        system_results = "-1 -1"
        example_results = "-1 -1"
        nuopc_pass = "-1"
        nuopc_fail = "-1"
        try:
            (subprocess.check_output(f"{command}", shell=True).strip().decode("utf-8"))
        except subprocess.CalledProcessError:

            example_results = "Build did not complete successfully"
            unit_results = "Build did not complete successfully"
            system_results = "Build did not complete successfully"
            nuopc_pass = "Build did not complete successfully"
            nuopc_fail = "Build did not complete successfully"
        try:
            make_info = (
                subprocess.check_output(
                    f"cat {self.build_dir}/module-build.log; cat {self.build_dir}/info.log",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError:
            make_info = (
                f"error finding {self.build_dir}/module-build.log or {self.build_dir}/info.log"
            )

        esmfmkfile = glob.glob(f"{self.build_dir}/lib/lib{permutation.build_type}/*/esmf.mk")
        _data = SummaryData(
            unit_results=unit_results,
            system_results=system_results,
            example_results=example_results,
            nuopc_pass=nuopc_pass,
            nuopc_fail=nuopc_fail,
            make_info=make_info,
            esmfmkfile=esmfmkfile,
        )
        self.create_summary(_data)
        git_cmd = f"cd {self.data.artifacts_root};git checkout {self.data.machine_name};git add {self.dirbranch}/{self.data.machine_name};git commit -a -m'update for build of {self.build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]';git push origin {self.data.machine_name}"
        print(f"git_cmd is {git_cmd}")
        self.runcmd(git_cmd)
        return

    def create_directory_structure(self):
        cmd = f"mkdir -p {self.outpath}/examples; rm {self.outpath}/examples/*; rm {self.outpath}/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {self.outpath}/apps; rm {self.outpath}/apps/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {self.outpath}/test; rm {self.outpath}/test/*"
        self.runcmd(cmd)
        cmd = f"mkdir -p {self.outpath}/lib; rm {self.outpath}/lib/*"
        self.runcmd(cmd)

    def collect_example_artifacts(self) -> List[Any]:
        permutation = self.build_permutation
        results = glob.glob(f"{self.build_dir}/examples/examples{permutation.build_type}/*/*.Log")
        results.extend(
            glob.glob(f"{self.build_dir}/examples/examples{permutation.build_type}/*/*.stdout")
        )
        return results

    def collect_example_results(self):
        permutation = self.build_permutation
        ex_result_file = glob.glob(
            f"{self.build_dir}/examples/examples{permutation.build_type}/*/*results"
        )

        results = "No examples ran"
        if len(ex_result_file) > 0:
            results = (
                subprocess.check_output(f"cat {ex_result_file[0]}", shell=True)
                .strip()
                .decode("utf-8")
            )
        return results

    def collect_test_artifacts(self) -> List[Any]:
        permutation = self.build_permutation
        results = glob.glob(f"{self.build_dir}/test/test{permutation.build_type}/*/*.Log")
        print("test_artifacts are ", results)
        results.extend(glob.glob(f"{self.build_dir}/test/test{permutation.build_type}/*/*.stdout"))
        return results

    def collect_unit_results(self):
        permutation = self.build_permutation
        results = "unit tests did not complete"
        try:
            results = (
                subprocess.check_output(
                    f"cat {self.build_dir}/test/test{permutation.build_type}/*/unit_tests_results",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            print("ERROR: ", error)
        return results

    def collect_system_results(self):
        permutation = self.build_permutation
        results = "system tests did not complete"
        try:
            results = (
                subprocess.check_output(
                    f"cat {self.build_dir}/test/test{permutation.build_type}/*/system_tests_results",
                    shell=True,
                )
                .strip()
                .decode("utf-8")
            )
        except subprocess.CalledProcessError as error:
            print("ERROR: ", error)
        return results

    def collect_nuopc_results(self) -> Tuple:
        nuopc_pass = 0
        nuopc_fail = 0
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
        except subprocess.CalledProcessError as error:
            print("ERROR: ", error)
        return nuopc_pass, nuopc_fail

    def copy_artifacts(self, oe_filelist):
        print(f"build_basename is {self.build_basename}")
        print(f"outpath is {self.outpath}")
        if not len(oe_filelist) == 0:
            return
        os.chdir(os.path.join(self.build_dir, CWD))

        if not self.is_test_stage(oe_filelist):
            self.nuke_old_files()

        self.generate_new_output_files(oe_filelist)
        if not self.is_test_stage(oe_filelist):
            self.final_stage()

        # Make directories, if they aren't already there
        self.create_directory_structure()

        print("globbing examples")
        example_artifacts = self.collect_example_artifacts()
        example_results = self.collect_example_results()
        test_artifacts = self.collect_test_artifacts()
        unit_results = self.collect_unit_results()
        system_results = self.collect_system_results()
        nuopc_pass, nuopc_fail = self.collect_nuopc_results()
        python_artifacts = glob.glob(f"{self.build_dir}/src/addon/ESMPy/*.log")

        os.chdir(self.build_dir)
        make_info = (
            subprocess.check_output("cat module-build.log; cat info.log", shell=True)
            .strip()
            .decode("utf-8")
        )
        os.chdir(CWD)

        print(f"esmfmkfile is {self.esmfmkfile}")
        _data = SummaryData(
            unit_results=unit_results,
            system_results=system_results,
            example_results=example_results,
            nuopc_pass=nuopc_pass,
            nuopc_fail=nuopc_fail,
            make_info=make_info,
            esmfmkfile=self.esmfmkfile,
        )
        self.create_summary(_data)
        timestamp = f"build time -- {self._build_time}"
        _generate_files(
            self.data.dryrun,
            example_artifacts,
            test_artifacts,
            python_artifacts,
            self.esmfmkfile,
            timestamp,
            self.outpath,
        )

        git_cmd = f"cd {self.data.artifacts_root};git checkout {self.data.machine_name};git add {self.dirbranch}/{self.data.machine_name};git commit -a -m'update for test of {self.build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]';git push origin {self.data.machine_name}"
        self.runcmd(git_cmd)
        return


def _generate_files(
    is_dryrun,
    example_artifacts,
    test_artifacts,
    python_artifacts,
    esmfmkfile,
    timestamp,
    outpath,
):
    for afile in example_artifacts:
        cmd = f"echo {timestamp} > {outpath}/examples/{os.path.basename(afile)}"
        _runcmd(cmd, is_dryrun)
        cmd = f"cat {afile} >> {outpath}/examples/{os.path.basename(afile)}"

        print(f"cmd is {cmd}")
        _runcmd(cmd, is_dryrun)
    for afile in test_artifacts:
        cmd = f"echo {timestamp} > {outpath}/test/{os.path.basename(afile)}"
        _runcmd(cmd, is_dryrun)
        cmd = f"cat {afile} >> {outpath}/test/{os.path.basename(afile)}"

        print(f"cmd is {cmd}")
        _runcmd(cmd, is_dryrun)
    for afile in esmfmkfile:
        cmd = f"echo {timestamp} > {outpath}/lib/{os.path.basename(afile)}"
        _runcmd(cmd, is_dryrun)
        cmd = f"cat {afile} >> {outpath}/lib/{os.path.basename(afile)}"
        print(f"cmd is {cmd}")
        _runcmd(cmd, is_dryrun)
    for afile in python_artifacts:
        cmd = f"echo {timestamp} > {outpath}/{os.path.basename(afile)}"
        _runcmd(cmd, is_dryrun)
        cmd = f"cat {afile} >> {outpath}/{os.path.basename(afile)}"

        print(f"cmd is {cmd}")
        _runcmd(cmd, is_dryrun)


def get_scheduler(scheduler: Scheduler) -> Scheduler:
    if scheduler == "pbs":
        return PBS()
    elif scheduler == "slurm":
        return Slurm()
    return NoScheduler()


def _runcmd(cmd, is_dry_run):
    if is_dry_run:
        print(f"would have executed {cmd}")
        return
    os.system(cmd)


def get_args():
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
    return vars(parser.parse_args())


if __name__ == "__main__":
    args = get_args()

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
