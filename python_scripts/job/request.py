
"""
Job Abstraction
"""


from pathlib import Path

import abc
import reprlib
from typing import Any, Dict, Generator, Iterable
import yaml
import json


class Base(abc.ABC):
    """Job Base Class"""
    def __init__(self, _type: str, **entries: Dict[str, Any]):
        self.type = _type
        self.__dict__.update(entries)

    def __repr__(self):
        return f"<{self.__class__.__name__} {reprlib.repr(self.__dict__)}/>"

    def __getattr__(self, item):
        return self.__dict__[item]

    def __str__(self):
        return str(self.__dict__)

    def __dir__(self) -> Iterable[str]:
        return [str(k) for k in self.__dict__]

    def to_json(self):
        """converts to json output"""
        return json.dumps(self.__dict__)


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
    def versions(self)-> Generator["Version", None, None]:
        """yields versions"""
        for item in self.__dict__["versions"]:
            yield Version(item, **self.__dict__["versions"][item])


class Version(Base):
    """holds version information"""


def read_yaml(_path: Path):
    with open(_path) as _file:
        return JobRequest("JOB", **yaml.safe_load(_file))
