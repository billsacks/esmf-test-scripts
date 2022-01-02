import argparse
import glob
import logging
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
        self._make_info = None
        self._build_succeeded = None

    @property
    def build_time(self) -> str:
        if self._build_time is not None:
            return self._build_time
        if self._esmfmkfile is None or len(self._esmfmkfile) < 1:
            self._build_time = datetime.now().strftime("%H:%M:%S")
        else:
            self._build_time = str(
                datetime.fromtimestamp(os.path.getmtime(self._esmfmkfile[0]))
            )
        return self._build_time

    @property
    def esmfmkfile(self) -> List[Any]:
        permutation = self.build_permutation
        if not self._esmfmkfile:
            self._esmfmkfile = glob.glob(
                f"{self.build_dir}/lib/lib{permutation.build_type}/*/esmf.mk"
            )
        return self._esmfmkfile

    @property
    def is_dry_run(self) -> bool:
        return str(self.is_dry_run).lower() == "true"

    @property
    def dir_branch(self) -> str:
        if not self._dir_branch:
            self._dir_branch = re.sub("/", "_", self.data.branch)
        return self._dir_branch

    @property
    def build_basename(self) -> str:
        if not self._build_basename:
            self._build_basename = os.path.basename(self.build_dir)
        return self._build_basename

    @property
    def scheduler(self) -> Scheduler:
        if not self._scheduler:
            self._scheduler = get_scheduler(self.data.scheduler)
        return self._scheduler

    @property
    def build_dir(self) -> str:
        if not self._build_dir:
            self._build_dir = f"{self.data.test_root_dir}/{self.data.build_basename}"
        return self._build_dir

    @property
    def esmf_os(self) -> str:
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
            self._build_permutation = BuildPermutation(
                compiler, version, mpiflavor, build_type
            )
        return self._build_permutation

    @property
    def build_hash(self) -> str:
        if not self._build_hash:
            self._build_hash = (
                subprocess.check_output("git describe --tags --abbrev=7", shell=True)
                .strip()
                .decode("utf-8")
            )
        return self._build_hash

    @property
    def build_succeeded(self) -> bool:
        if not self._build_succeeded:
            command = f"grep success {self.build_dir}/build_{self.data.jobid}.log"
            try:
                self._build_succeeded = bool(
                    subprocess.check_output(f"{command}", shell=True)
                    .strip()
                    .decode("utf-8")
                )
            except subprocess.CalledProcessError:
                self._build_succeeded = False
        return self._build_succeeded

    @property
    def build_failed(self) -> bool:
        return not self.build_succeeded

    @property
    def outpath(self) -> str:
        if not self._outpath:
            p = self.build_permutation  # pylint: disable=invalid-name
            self._outpath = f"{self.data.artifacts_root}/{self.dir_branch}/{self.data.machine_name}/{p.compiler}/{p.version}/{p.build_type}/{p.mpiflavor}"
            if self.data.mpiversion != "None":
                self._outpath = self._outpath + f"/{self.data.mpiversion}"
        return self._outpath

    @property
    def make_info(self) -> str:
        if not self._make_info:
            try:
                self._make_info = (
                    subprocess.check_output(
                        f"cat {self.build_dir}/module-build.log; cat {self.build_dir}/info.log",
                        shell=True,
                    )
                    .strip()
                    .decode("utf-8")
                )
            except subprocess.CalledProcessError:
                self._make_info = f"error finding {self.build_dir}/module-build.log or {self.build_dir}/info.log"
        return self._make_info

    def monitor(self) -> None:
        logging.info("dryrun is %s", self.is_dry_run)
        start_time = time.time()
        seconds = 60 * 60 * 40  # 40 hours?
        while True:
            current_time = time.time()
            elapsed_time = current_time - start_time
            job_done = self.scheduler.checkqueue(self.data.jobid)
            if job_done:
                oe_filelist = self._fetch_oe_filelist()
                logging.info(f"filelist is {oe_filelist}")
                logging.info(f"oe list is {oe_filelist}\n")
                self.copy_artifacts(oe_filelist)
                break
            time.sleep(30)

            if elapsed_time > seconds:
                logging.info(
                    "Finished iterating in: %s seconds", str(int(elapsed_time))
                )
                break

    def _fetch_oe_filelist(self) -> List[Any]:
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
        return oe_filelist

    def _create_summary(self) -> None:
        logging.info("esmf_os is %s", self.esmf_os)
        lines = [
            HORIZONTAL_LINE,
            self._get_summary_header(),
            f"Build time = {self.build_time}\n",
            f"git hash = {self.build_hash}\n\n",
            self._get_test_results(),
            HORIZONTAL_LINE,
            f"\n\n{self.make_info}\n\n",
            HORIZONTAL_LINE,
        ]
        with open(f"{self.outpath}/summary.dat", "w") as _file:
            _file.writelines(lines)

    def _is_test_stage(self, oe_filelist) -> bool:
        for cfile in oe_filelist:
            logging.info("cfile is %s", cfile)
            if int(self.data.jobid) < 0:
                return True
            if (
                cfile.find(f"test_{self.data.jobid}") != -1
            ):  # this is just the build job, so no test artifacts yet
                return True
        return False

    def _nuke_old_files(self) -> None:
        # remove old files in out directory
        logging.info("removing old files for build stage")
        cmds = [
            f"mkdir -p {self.outpath}/out",
            f"rm {self.outpath}/*/*",
            f"rm {self.outpath}/*.log",
            f"rm {self.outpath}/summary.dat",
        ]
        logging.info("nuking old files in %s", self.outpath)
        _runcmd(";".join(cmds), "false")

    def _generate_new_output_files(self, oe_filelist) -> None:
        for cfile in oe_filelist:
            nfile = os.path.basename(re.sub(f"_{self.data.jobid}", "", cfile))
            _runcmd(f"echo `date` > {self.outpath}/out/{nfile}", self.is_dry_run)
            _runcmd(f"cat {cfile} >> {self.outpath}/out/{nfile}", self.is_dry_run)

    def _run_final_stage(self) -> None:
        self._create_summary()
        git_cmd = [
            f"cd {self.data.artifacts_root}",
            f"git checkout {self.data.machine_name}",
            f"git add {self.dir_branch}/{self.data.machine_name}",
            f"git commit -a -m'update for build of {self.build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]'",
            f"git push origin {self.data.machine_name}",
        ]

        logging.info("git_cmd is %s", git_cmd)
        _runcmd(";".join(git_cmd), self.is_dry_run)

    def create_directory_structure(self) -> None:
        logging.info("creating directory structure at %s", self.outpath)
        cmds = [
            f"mkdir -p {self.outpath}/examples; rm {self.outpath}/examples/*; rm {self.outpath}/*",
            f"mkdir -p {self.outpath}/apps; rm {self.outpath}/apps/*",
            f"mkdir -p {self.outpath}/test; rm {self.outpath}/test/*",
            f"mkdir -p {self.outpath}/lib; rm {self.outpath}/lib/*",
        ]
        _runcmd(";".join(cmds), self.is_dry_run)

    def collect_example_artifacts(self) -> List[Any]:
        permutation = self.build_permutation
        return [
            *glob.glob(
                f"{self.build_dir}/examples/examples{permutation.build_type}/*/*.Log"
            ),
            *glob.glob(
                f"{self.build_dir}/examples/examples{permutation.build_type}/*/*.stdout"
            ),
        ]

    def collect_example_results(self) -> str:
        if self.build_failed:
            return "Build was not successful"
        permutation = self.build_permutation
        ex_result_file = glob.glob(
            f"{self.build_dir}/examples/examples{permutation.build_type}/*/*results"
        )
        results = "Example tests queued"
        if len(ex_result_file) > 0:
            results = (
                subprocess.check_output(f"cat {ex_result_file[0]}", shell=True)
                .strip()
                .decode("utf-8")
            )
        return results

    def collect_test_artifacts(self) -> List[Any]:
        permutation = self.build_permutation
        return [
            *glob.glob(f"{self.build_dir}/test/test{permutation.build_type}/*/*.Log"),
            *glob.glob(
                f"{self.build_dir}/test/test{permutation.build_type}/*/*.stdout"
            ),
        ]

    def collect_unit_results(self) -> str:
        if self.build_failed:
            return "Build was not successful"
        permutation = self.build_permutation
        results = "Unit tests queued"
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
            logging.error(error)
        return results

    def collect_system_results(self) -> str:
        if self.build_failed:
            return "Build was not successful"
        permutation = self.build_permutation
        results = "System tests queued"
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
            logging.error(error)
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
            logging.error(error)
        return nuopc_pass, nuopc_fail

    def collect_python_artifacts(self) -> List[Any]:
        return glob.glob(f"{self.build_dir}/src/addon/ESMPy/*.log")

    def _get_test_results(self) -> List[str]:
        results = []
        nuopc_pass, nuopc_fail = self.collect_nuopc_results()
        unit_results = re.sub(" FAIL", "\tFAIL", self.collect_unit_results())
        system_results = re.sub(" FAIL", " \tFAIL", self.collect_system_results())
        example_results = re.sub(" FAIL", " \tFAIL", self.collect_example_results())
        results.append(f"unit test results   \t{unit_results}\n")
        results.append(f"system test results \t{system_results}\n")
        results.append(f"example test results \t{example_results}\n")
        results.append(
            f"nuopc test results \tPASS {nuopc_pass} \tFAIL {nuopc_fail}\n\n"
        )
        return results

    def copy_artifacts(self, oe_filelist) -> None:
        logging.info(
            "build_basename is %s, outpath is %s", self.build_basename, self.outpath
        )
        if not len(oe_filelist) == 0:
            return
        os.chdir(os.path.join(self.build_dir, CWD))

        if not self._is_test_stage(oe_filelist):
            self._nuke_old_files()

        self._generate_new_output_files(oe_filelist)

        if not self._is_test_stage(oe_filelist):
            self._run_final_stage()

        self.create_directory_structure()

        os.chdir(os.path.join(self.build_dir, CWD))

        self._create_summary()
        self._generate_artifact_files()
        self._push_artifacts()

    def _push_artifacts(self) -> None:
        git_cmds = [
            f"cd {self.data.artifacts_root}",
            f"git checkout {self.data.machine_name}",
            f"git add {self.dir_branch}/{self.data.machine_name}",
            f"git commit -a -m'update for test of {self.build_basename} with hash {self.build_hash} on {self.data.machine_name} [ci skip]'",
            f"git push origin {self.data.machine_name}",
        ]
        _runcmd(";".join(git_cmds), self.is_dry_run)

    def _generate_artifact_files(self) -> None:

        example_artifacts = self.collect_example_artifacts()
        test_artifacts = self.collect_test_artifacts()
        python_artifacts = self.collect_python_artifacts()
        esmfmkfile = self.esmfmkfile

        queue = zip(
            ["examples", "test", "lib", ""],
            [example_artifacts, test_artifacts, esmfmkfile, python_artifacts],
        )

        timestamp = f"build time -- {self.build_time}"
        for _path, _task in queue:
            for afile in _task:
                abs_path = os.path.join(self.outpath, _path, os.path.basename(afile))
                cmds = [
                    f"echo {timestamp} > {abs_path}",
                    f"cat {afile} >> {abs_path}",
                ]
                _runcmd(
                    ";".join(cmds),
                    self.is_dry_run,
                )

    def _get_summary_header(
        self,
    ) -> str:
        return f"Build for = {self.data.build_basename}, mpi version {self.data.mpiversion} on {self.data.machine_name} esmf_os: {self.esmf_os}\n"


def get_scheduler(scheduler: Scheduler) -> Scheduler:
    if scheduler == "pbs":
        return PBS()
    if scheduler == "slurm":
        return Slurm()
    return NoScheduler()


def _runcmd(cmd, is_dry_run):
    if is_dry_run:
        logging.info("would have executed %s", cmd)
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
    archiver.monitor()
