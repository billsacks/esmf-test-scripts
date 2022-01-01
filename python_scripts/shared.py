import os
import subprocess
import shutil

from collections import namedtuple


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


def update_repo(subdir, branch, nuopcbranch, is_dryrun=False):
    os.system(f"rm -rf {subdir}")
    if not os.path.isdir(subdir):

        cmdstring = f"git clone -b {branch} git@github.com:esmf-org/esmf {subdir}"
        nuopcclone = (
            f"git clone -b {nuopcbranch} git@github.com:esmf-org/nuopc-app-prototypes"
        )
        if is_dryrun is True:
            print(f"would have executed {cmdstring}")
            print(f"would have executed {nuopcclone}")
            print(f"would have cd'd to {subdir}")
            return

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


def rmdir(path):
    shutil.rmtree(path)


def _runcmd(cmd, is_dry_run=True):
    if is_dry_run:
        print(f"would have executed {cmd}")
        return
    os.system(cmd)
