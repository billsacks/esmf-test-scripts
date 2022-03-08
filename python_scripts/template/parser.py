"""
Parses and hydrates python string templates
"""

import collections
import os


JobProperties = collections.namedtuple(
    "JobAttributes", ["filename", "queue", "cpn", "time", "partition", "cluster"]
)


class Header:
    """represents instruction header"""

    job_id: str
    bash: str

    def __init__(self, props: JobProperties):
        self.props = props

    def bash_header_line(self) -> str:
        """default bash header line of #!/usr/bin/derp/derp"""
        return f"#!{self.bash} -l\n"

    def text(self) -> str:
        """returns header as string with '\n'"""
        return "\n".join(
            [
                self.bash_header_line(),
                "export JOBID=$1",
            ]
        )

    @property
    def account(self) -> str:
        """returns the users local linux account id"""
        # TODO Linux get account ID
        return ""


class Slurm(Header):
    """slurm scheduler instructions"""

    def text(self) -> str:
        return self.bash_header_line() + self.hydrate()

    def hydrate(self) -> str:
        """hydrates the template with data"""
        return f"""
    #SBATCH --account={self.account}
    #SBATCH -o {self.props.filename}_%j.o
    #SBATCH -e test-intel_2020_intelmpi_g.bat_%j.e
    #SBATCH --time={self.props.time}
    #SBATCH --partition={self.props.partition}
    #SBATCH --cluster={self.props.cluster}
    #SBATCH --qos={self.props.queue}
    #SBATCH --nodes=1
    #SBATCH --ntasks-per-node={self.props.cpn}
    #SBATCH --exclusive
    export JOBID=$SLURM_JOBID
    """


class PBS(Header):
    """slurm scheduler instructions"""

    def __init__(self, props: JobProperties):
        self.props = props

    def text(self) -> str:
        return self.bash_header_line() + self.hydrate()

    def hydrate(self) -> str:
        """hydrates the template with data"""
        return f"""
    #PBS -N {self.props.filename}
    #PBS -l walltime={self.props.time}
    #PBS -q {self.props.queue}
    #PBS -A {self.account}
    #PBS -l select=1:ncpus={self.props.cpn}:mpiprocs={self.props.cpn}
    JOBID="`echo $PBS_JOBID | cut -d. -f1`\n\n"
    cd {os.getcwd()}\n
    """
