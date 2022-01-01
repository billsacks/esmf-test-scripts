# pylint: disable=unspecified-encoding

import argparse
import os
from test import ESMFTest, ESMFTestData

from config_reader_yaml import fetch_yaml_properties
from shared import rmdir


def get_args():
    """get_args

    Returns:
        list:
    """
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


def _reclone(_test: ESMFTest):
    print("rcloning")
    rmdir(_test.artifacts_root)
    os.system("git clone https://github.com/esmf-org/esmf-test-artifacts.git")
    os.chdir("esmf-test-artifacts")
    os.system(f"git checkout -b {_test.machine_name}")
    os.chdir("..")


if __name__ == "__main__":
    args = get_args()

    _data = ESMFTestData(
        args["yaml"], args["artifacts"], args["workdir"], args["dryrun"]
    )
    machine_properties = fetch_yaml_properties(
        machine_yaml_config_path=os.path.dirname(args["yaml"]),
        global_yaml_config_path=os.path.join(
            os.path.dirname(args["yaml"]), "global.yaml"
        ),
    )

    test = ESMFTest(_data, machine_properties)

    if test.do_reclone:
        _reclone(test)

    test.create_job_card_and_submit()
