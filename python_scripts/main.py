import os

import argparse
from pathlib import Path
import pathlib
from job.scheduler import Scheduler, from_job_request
import job.request as request
from scheduler import scheduler
from noscheduler import NoScheduler
from pbs import pbs
from slurm import slurm


REPO_ESMF_TEST_ARTIFACTS = "https://github.com/esmf-org/esmf-test-artifacts.git"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Archive collector for ESMF testing framework"
    )
    parser.add_argument(
        "-w",
        "--workdir",
        help="directory where builds will be mad #",
        default=os.getcwd(),
    )
    parser.add_argument(
        "-y",
        "--yaml",
        help="Yaml file defining builds and testing parameters",
    )
    parser.add_argument(
        "-a",
        "--artifacts",
        help="directory where artifacts will be placed",
    )
    parser.add_argument(
        "-d",
        "--dryrun",
        help="directory where artifacts will be placed",
        default=False,
    )
    args = vars(parser.parse_args())

    request = request.read_yaml(pathlib.Path("../config/cheyenne.yaml"))
    scheduler = from_job_request(request)
    print(scheduler)
    with open(pathlib.Path("./testing.txt"), "w") as _file:
        _file.write(scheduler.text())
    print(scheduler.text())
    exit()
    for k, v in request.items():
        print(k, v)
    with open("./test.txt", "w") as _file:
        pass
        # _file.write(scheduler.text())
