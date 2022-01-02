# pylint: disable=unspecified-encoding

import subprocess
from collections import namedtuple

from schedulers.scheduler import Scheduler

TemplateData = namedtuple(
    "TemplateData",
    [
        "partition",
        "filename",
        "account",
        "time",
        "constraint",
        "cluster",
        "queue",
        "cpn",
    ],
)


class Slurm(Scheduler):

    TEMPLATE_PATH = "./templates/sbatch.build.bat.template"
    SCHEDULER_TYPE = "slurm"

    @property
    def template_data(self) -> TemplateData:
        # May not need empty strings for defaults with defaultdict
        return TemplateData(
            filename=self.data.get("filename", ""),
            partition=_format_partition(self.data.get("partition", "")),
            account=self.data.get("account", ""),
            time=self.data.get("time", ""),
            cluster=_format_cluster(self.data.get("cluster", "")),
            constraint=_format_constraint(self.data.get("constraint", "")),
            queue=self.data.get("queue", ""),
            cpn=self.data.get("cpn", ""),
        )

    def check_queue(self, job_id):
        return _check_queue(job_id)


class slurm(Slurm):  # pylint: disable=invalid-name
    """slurm is a wrapper around Slurm to maintain backwards compatibility"""


def _check_queue(jobid):
    if int(jobid) < 0:
        return True
    queue_query = (
        f"sacct -j {jobid} | head -n 3 | tail -n 1 | awk -F ' ' '{{print $6}}'"
    )
    try:
        result = (
            subprocess.check_output(queue_query, shell=True).strip().decode("utf-8")
        )
        return result in ["COMPLETED", "TIMEOUT", "FAILED", "CANCELLED"]
    except subprocess.CalledProcessError:
        return True


def _format_constraint(value):
    if not value:
        return ""
    return f"SBATCH -C {value}"


def _format_partition(value):
    if not value:
        return ""
    return f"#SBATCH --partition={value}"


def _format_cluster(value):
    # SBATCH --cluster={}\n
    if not value:
        return ""
    return f"#SBATCH --cluster={value}"


if __name__ == "__main__":
    pass
