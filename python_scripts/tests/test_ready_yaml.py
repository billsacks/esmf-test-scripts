# pylint: disable=missing-function-docstring
import json
from pathlib import Path
import job.request as _job


def test_read_yaml():
    expected = {
        "type": "7.4.0",
        "compiler": "gnu/7.4.0",
        "netcdf": "netcdf/4.7.3",
        "mpi": {"openmpi": {"module": "openmpi/4.0.3"}},
        "extra_env_vars": {"var1": "ESMF_F90COMPILER=mpif90"},
    }

    job = _job.read_yaml(Path("./tests/fixtures/cheyenne.yaml"))
    compiler = list(job.compilers)[0]
    actual = list(compiler.versions)[0].__dict__

    assert actual == expected


def test_to_json():

    with open(Path("./tests/fixtures/cheyenne.json")) as _file:

        expected = json.load(_file)
        actual = json.loads(
            _job.read_yaml(Path("./tests/fixtures/cheyenne.yaml")).to_json()
        )

        print(actual)
        assert False
        assert actual == expected


"""
{
	'type': 'JOB',
	'machine': 'cheyenne',
	'account': 'p93300606',
	'queue': 'regular',
	'partition': 'None',
	'scheduler': 'pbs',
	'corespernode': 36,
	'compiler': ['gfortran', 'intel'],
	'branch': ['develop'],
	'headnodename': 'cheyenne6',
	'gfortran': {
		'test_time': '2:00:00',
		'versions': {
			'7.4.0': {
				'compiler': 'gnu/7.4.0',
				'netcdf': 'netcdf/4.7.3',
				'mpi': {
					'openmpi': {
						'module': 'openmpi/4.0.3'
					}
				},
				'extra_env_vars': {
					'var1': 'ESMF_F90COMPILER=mpif90'
				}
			},
			'9.1.0': {
				'compiler': 'gnu/9.1.0',
				'netcdf': 'netcdf/4.7.3',
				'mpi': {
					'openmpi': {
						'module': 'openmpi/4.0.5'
					},
					'mpt': {
						'module': 'mpt/2.22'
					}
				},
				'extra_env_vars': {
					'var1': 'ESMF_F90COMPILER=mpif90'
				}
			},
			'10.1.0': {
				'compiler': 'gnu/10.1.0',
				'netcdf': 'netcdf/4.7.4',
				'mpi': {
					'openmpi': {
						'module': 'openmpi/4.0.5'
					},
					'mpt': {
						'module': 'mpt/2.23'
					}
				},
				'extra_env_vars': {
					'var1': 'ESMF_F90COMPILEOPTS="-fallow-argument-mismatch -fallow-invalid-boz"',
					'var2': 'ESMF_F90COMPILER=mpif90'
				}
			}
		}
	},
	'intel': {
		'test_time': '3:00:00',
		'versions': {
			'18.0.5': {
				'compiler': 'intel/18.0.5',
				'netcdf': 'netcdf/4.6.3',
				'mpi': {
					'mpiuni': {
						'module': 'None'
					}
				},
				'mpt': {
					'module': 'mpt/2.19'
				},
				'openmpi': {
					'module': 'openmpi/3.1.4',
					'pythontest': True
				},
				'intelmpi': {
					'module': 'impi/2018.4.274',
					'pythontest': True
				}
			}
		},
		'extramodule': 'python'
	}
}
"""
