"""
__author__: Mark Potts <mark.potts.@noaa.gov> Ryan Long <ryan.long@noaa.gov>


"""
# pylint: disable=unspecified-encoding

import logging
import os
import subprocess
from collections import namedtuple
from string import Template

from schedulers.scheduler import Scheduler, TestData

TemplateData = namedtuple(
    "TemplateData",
    [
        "b_filename",
        "t_filename",
        "build_time",
        "test_time",
        "filename_and_time",
        "queue",
        "account",
        "cpn",
        "cwd",
    ],
)

ScriptType = namedtuple("ScriptType", ["handler", "type"])


class PBS(Scheduler):
    """PBS Scheduler Type"""

    TEMPLATE_PATH = "./templates/pbs.build.bat.template"
    SCHEDULER_TYPE = "pbs"

    @property
    def template_data(self) -> TemplateData:
        # May not need empty strings for defaults with defaultdict
        return TemplateData(
            b_filename=self.data.get("b_filename", ""),
            t_filename=self.data.get("t_filename", ""),
            build_time=self.data.get("build_time", ""),
            test_time=self.data.get("test_time", ""),
            filename_and_time="",
            queue=self.data.get("queue", ""),
            account=self.data.get("account", ""),
            cpn=self.data.get("cpn", ""),
            cwd=os.getcwd(),
        )

    def create_headers(self, test: TestData):  # pylint: disable=invalid-name
        for file_info in [
            ScriptType(test.fb, "build"),
            ScriptType(test.ft, "test"),
        ]:
            logging.debug("creating headers for %s", file_info)
            _create_headers(self.template_data, file_info)

    def check_queue(self, job_id):  # pylint: disable=invalid-name
        return _check_queue(job_id)

    def _batch_test(self, job_number, test_filename):
        # submit the second job to be dependent on the first
        batch_test_cmd = f"qsub -W depend=afterok:{job_number} {test_filename}"
        print(f"Submitting test_batch with command: {batch_test_cmd}")
        return PBS.fetch_job_number(test_filename)


class pbs(PBS):  # pylint: disable=invalid-name
    """pbs is a wrapper around PBS to maintain backwards compatibility"""


def _create_headers(template_data: TemplateData, file_info: ScriptType):
    file_handler, file_type = file_info

    def filename_and_time_template(filename, build_time):
        return f"""
        #PBS -N {filename}\n 
        #PBS -l walltime={build_time}\n
        """

    result = None
    with open(PBS.TEMPLATE_PATH, "r") as _template:
        if file_type == "build":
            template_data._replace(
                filename_and_time=filename_and_time_template(
                    template_data.b_filename, template_data.build_time
                )
            )
        else:
            template_data._replace(
                filename_and_time=filename_and_time_template(
                    template_data.t_filename, template_data.test_time
                )
            )
        src = Template(_template.read())
        result = src.safe_substitute(template_data._asdict())
        file_handler.writelines(result)


def _check_queue(job_id):
    # TODO abstract in root class
    if int(job_id) < 0:
        return True
    queue_query = f"qstat -H {job_id} | tail -n 1 | awk -F ' +' '{{print $10}}'"
    try:
        result = (
            subprocess.check_output(queue_query, shell=True).strip().decode("utf-8")
        )
        return result in ["F"]
    except subprocess.CalledProcessError:
        return True
