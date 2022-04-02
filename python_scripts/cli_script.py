import collections
import os
import pathlib
from typing import List, Dict, Any


# Keys
HDF5_KEY = "hdf5"
NETCDF_FORTRAN_KEY = "netcdf-fortran"
COMPILER_VERSIONS_KEY = "versions"
EXTRA_ENV_VARS_KEY = "extra_env_vars"
EXTRA_COMMANDS_KEY = "extra_commands"
EXTRA_MODULE_KEY = "extramodule"
MODULE_PATH_KEY = "modulepath"
UNLOAD_MODULE_KEY = "unloadmodule"
BUILD_KEY = "build"
TEST_KEY = "test"
PYTHON_TEST_KEY = "pythontest"

HEADER_LIST = [BUILD_KEY, TEST_KEY]

ScriptProps = collections.namedtuple("ScriptProps",
                                     ['machine_list', 'compiler', 'version', 'bash_command', 'mpi_key', 'mpidict',
                                      'c_version', 'header_type', 'cpn', 'build_type', 'mpi_flavor', 'head_node_name',
                                      'mpi_version', 'key'])


class Script(collections.UserList):
    """represents a script as a line by line list of its contents"""

    def __init__(self):
        super().__init__()
        self.name = None

    def write(self, path: pathlib.Path):
        with open(path) as _file:
            _file.writelines(super())

    @staticmethod
    def script_name(props: ScriptProps):
        _map = {
            BUILD_KEY.lower(): f"{BUILD_KEY}-{props.compiler}_{props.c_version}_{props.key}_{props.build_type}.bat",
            TEST_KEY.lower(): f"{TEST_KEY}-{props.compiler}_{props.c_version}_{props.key}_{props.build_type}.bat"
        }
        return _map[props.build_type]


class CliScriptGenerator:
    def __init__(self, mpi_dictionary: Dict[str, Any], key: Any, machine_list: List[str], compiler, bash_command: str,
                 head_node_name: str):
        self.mpi_dictionary = mpi_dictionary
        self.key = key
        self.machine_list = machine_list
        self.compiler = compiler
        self.bash_command = bash_command
        self.head_node_name = head_node_name

    def props(self, header_type):
        return ScriptProps(self.machine_list, self.compiler, "version", self.bash_command, self.key,
                           self.mpi_dictionary,
                           "c_version", header_type, "cpn", "build_type", self.mpi_flavor, self.head_node_name,
                           "mpi_version", self.key)

    @property
    def mpi_flavor(self):
        if is_missing_mpi_flavor(self.mpi_dictionary[self.key]):
            return {"module": ""}
        return self.mpi_dictionary[self.key]

    @property
    def header_list(self):
        if self.mpi_flavor is not None and PYTHON_TEST_KEY in self.mpi_flavor:
            return HEADER_LIST + ["python"]
        return HEADER_LIST

    def create_scripts(self):

        for headerType in self.header_list:
            props = self.props(headerType)
            script = Script()
            script.extend([
                add_python_test_header(props),
                add_module_loading(props),
                add_mpi_configuration(props),
                add_netcdf_configuration(props),
                add_hdf5_configuration(props),
                add_netcdf_fortran_configuration(props),
                add_generate_module_list(props),
                "set -x\n",
                add_extra_environment_variables(props),
                add_extra_commands(props),
                f"export ESMF_DIR={os.getcwd()}\n",
                f"export ESMF_COMPILER={self.compiler}\n",
                f"export ESMF_COMM={self.key}\n"
                f"export ESMF_BOPT='{props.build_type}'\n",
                "export ESMF_TESTEXHAUSTIVE='ON'\n",
                "export ESMF_TESTWITHTHREADS='ON'\n",
                add_make_configuration(props),
                add_python_testing(props)
            ])
            script.name = Script.script_name(props)
            script.write(pathlib.Path("./testing.txt"))


def add_generate_module_list(props: ScriptProps) -> List[str]:
    if props.header_type == BUILD_KEY:
        return ["module list >& module-build.log\n\n"]
    return ["module list >& module-test.log\n\n"]


def add_python_testing(props: ScriptProps) -> List[str]:
    results = []
    if (PYTHON_TEST_KEY in props.mpi_flavor) and (props.header_type == TEST_KEY):
        results.extend([
            "\ncd ../src/addon/ESMPy\n",
            "\nexport PATH=$PATH:$HOME/.local/bin\n",
            "python3 setup.py build 2>&1 | tee python_build.log\n",
            f"ssh {props.head_node_name} {os.getcwd()}/runpython.sh 2>&1 | tee python_build.log\n",
            "python3 setup.py test 2>&1 | tee python_test.log\n",
            "python3 setup.py test_examples 2>&1 | tee python_examples.log\n",
            "python3 setup.py test_regrid_from_file 2>&1 | tee python_regrid.log\n"
        ])
    return results


def add_make_configuration(props: ScriptProps) -> List[str]:
    results = []
    if props.header_type == BUILD_KEY:
        results.append(f"make -j {props.cpn} 2>&1| tee build_$JOBID.log\n\n")
    elif props.header_type == TEST_KEY:
        results.extend([
            f"make info 2>&1| tee info.log \n",
            "make install 2>&1| tee install_$JOBID.log \n",
            "make all_tests 2>&1| tee test_$JOBID.log \n",
            "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n"
        ])
        if props.mpi_flavor["module"] != "None":
            results.append(
                "chmod +x runpython.sh\ncd nuopc-app-prototypes\n./testProtos.sh 2>&1| tee ../nuopc_$JOBID.log \n\n")
    else:
        results.append("python3 setup.py test_examples_dryrun\npython3 setup.py test_regrid_from_file_dryrun\n")
    return results


def add_extra_commands(props: ScriptProps) -> List[str]:
    results = []
    if EXTRA_COMMANDS_KEY in props.machine_list[props.compiler]["versions"][props.c_version]:
        for cmd in props.machine_list[props.compiler]["versions"][props.c_version][EXTRA_COMMANDS_KEY]:
            results.append(
                f"{props.machine_list[props.compiler]['versions'][props.c_version]['extra_commands'][cmd]}\n")
    return results


def add_extra_environment_variables(props: ScriptProps) -> List[str]:
    results = []
    if EXTRA_ENV_VARS_KEY in props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version]:
        for var in props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version][EXTRA_ENV_VARS_KEY]:
            results.append(
                f"export {props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version][EXTRA_ENV_VARS_KEY][var]}\n")
    return results


def add_netcdf_fortran_configuration(props: ScriptProps) -> List[str]:
    results = []
    if NETCDF_FORTRAN_KEY in props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version]:
        results.append(
            f"module load {props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version][NETCDF_FORTRAN_KEY]} \n")
    return results


def add_hdf5_configuration(props: ScriptProps) -> List[str]:
    results = []
    if HDF5_KEY in props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version]:
        hdf5 = props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version][HDF5_KEY]
        results.append(f"module load {hdf5} \n")
    return []


def add_netcdf_configuration(props: ScriptProps) -> List[str]:
    compiler = props.machine_list[props.compiler][COMPILER_VERSIONS_KEY][props.c_version]["compiler"]
    module = props.mpi_flavor["module"]
    netcdf_version = props.machine_list[compiler][COMPILER_VERSIONS_KEY][props.c_version]["netcdf"]
    if props.machine_list[compiler][COMPILER_VERSIONS_KEY][props.c_version]["netcdf"] == "None":
        return [f"module load {compiler} {module} \n\n"]
    return [f"module load {compiler} {module} {netcdf_version}\n",
            "export ESMF_NETCDF=nc-config\n\n",
            ]


def add_mpi_configuration(props: ScriptProps) -> List[str]:
    results = []
    if "mpi_env_vars" in props.mpi_flavor:
        for mpi_var in props.mpi_flavor["mpi_env_vars"]:
            results.append(
                "export {}\n".format(props.mpidict[props.key]["mpi_env_vars"][mpi_var])
            )
    return results


def is_missing_mpi_flavor(mpi_flavor):
    return mpi_flavor is None or "module" not in mpi_flavor or mpi_flavor["module"] in [
        None, "None", "none"]


def add_module_loading(props: ScriptProps) -> List[str]:
    results = []
    if UNLOAD_MODULE_KEY in props.machine_list[props.compiler]:
        results.append(f"\nmodule unload {props.machine_list[props.compiler][UNLOAD_MODULE_KEY.lower()]}\n")
    if MODULE_PATH_KEY in props.machine_list:
        results.append(f"\nmodule use {props.machine_list[MODULE_PATH_KEY.lower()]}\n")
    if EXTRA_MODULE_KEY in props.machine_list[props.compiler]:
        results.append(
            f"\nmodule load {props.machine_list[props.compiler][EXTRA_MODULE_KEY.lower()]}\n"
        )
    return results


def add_python_test_header(props: ScriptProps) -> List[str]:
    return [
        "#!{} -l\n".format(props.bash_command),
        "cd {}\n".format(os.getcwd()),
        "export ESMFMKFILE=`find $PWD/DEFAULTINSTALLDIR -iname esmf.mk`\n\n",
        "cd {}/src/addon/ESMPy\n".format(os.getcwd())
    ]
