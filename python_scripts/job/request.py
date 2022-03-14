"""
Job Abstraction
"""


import abc
import json
import os
import reprlib
from pathlib import Path
from typing import Any, Dict, Generator, Iterable

import yaml


DEFAULT_TIME = "1:00:00"

defaults = {
    "machine_name": None,
    "git-https": True,  # always true
    "https": True,  # always true
    "bash": "/bin/bash",
    "account": None,
    "partition": "none",
    "queue": None,
    "headnodename": f"{os.uname()[1]}",
    "nuopcbranch": "develop",
    "build_types": ["O", "g"],  # static
    "script_dir": os.getcwd(),
    "cluster": None,
    "constraint": None,
    "subdir": "",
    "build_time": DEFAULT_TIME,
    "test_time": DEFAULT_TIME,
    "time": "3:00:00",  # TODO
    "filename": "temp",  # TODO
    "cpn": "cpn",  # TODO
}


class Base(abc.ABC):
    """Job Base Class"""

    def __init__(self, _type: str, **kwds):
        print(kwds)
        self.type = _type
        self.__dict__.update(**kwds)

    def __repr__(self):
        return f"<{self.__class__.__name__} {reprlib.repr(self.__dict__)}/>"

    def __getattr__(self, item):
        return self.__dict__.get(item, defaults[item])

    def __str__(self):
        return str(self.__dict__)

    def __dir__(self) -> Iterable[str]:
        return [str(k) for k in self.__dict__]

    def to_json(self):
        """converts to json output"""
        return json.dumps(self.__dict__)

    def items(self):
        """itterator over key/value pairs"""
        return self.__dict__.items()


class JobRequest(Base):
    """represent a job request"""

    @property
    def compilers(self):
        """yields compilers"""
        for item in self.__dict__["compiler"]:
            yield Compiler(item, **self.__getattribute__(item))


class Compiler(Base):
    """holds compiler information"""

    @property
    def versions(self) -> Generator["Version", None, None]:
        """yields versions"""
        for item in self.__dict__["versions"]:
            yield Version(item, **self.__dict__["versions"][item])


class Version(Base):
    """holds version information"""


def read_yaml(_path: Path) -> JobRequest:
    """reads yaml file into JobRequest instance"""
    with open(_path, "r") as _file:
        return JobRequest("Job", **yaml.safe_load(_file))
