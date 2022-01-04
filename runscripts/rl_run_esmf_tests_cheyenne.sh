#!/bin/bash -l

cd /glade/u/home/rlong/scratch/esmf-testing/esmf-test-scripts
git remote update
git pull -X theirs --no-edit origin
module load python
cd /glade/u/home/rlong/scratch/esmf-testing
python3 -m 
/esmf-test-scripts/python_scripts/esmf_test.py -y  ./esmf-test-scripts/config/rl_cheyenne.yaml -a $PWD/esmf-test-artifacts


