# pylint: disable=unspecified-encoding
import os
import platform
from collections import namedtuple
from typing import NamedTuple

import yaml
from shared import namedtuple_with_defaults


def uname():
    """Safe for Windows"""
    assert platform.system() in ["Linux", "Darwin"]
    return getattr(os, "uname")()[1]


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

MergedProperties = namedtuple(
    "MergedProperties", GlobalProperties._fields + MachineProperties._fields
)


def fetch_yaml_properties(*, global_yaml_config_path, machine_yaml_config_path) -> NamedTuple:
    with open(global_yaml_config_path) as file:
        global_properties = GlobalProperties(**yaml.load(file, Loader=yaml.SafeLoader))
    with open(machine_yaml_config_path) as file:

        machine_properties = MachineProperties(**yaml.load(file, Loader=yaml.SafeLoader))
    return MergedProperties(*(global_properties + machine_properties))
