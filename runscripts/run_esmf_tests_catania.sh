#!/bin/bash -l

cd /project/esmf/esmf-testing/esmf-test-scripts
git remote update
git pull -X theirs --no-edit 
cd /project/esmf/esmf-testing
python3 /project/esmf/esmf-testing/esmf-test-scripts/python_scripts/test_esmf.py -y  /project/esmf/esmf-testing/esmf-test-scripts/config/catania.yaml -a /project/esmf/esmf-testing/esmf-test-artifacts >& /project/esmf/esmf-testing/test_esmf_catania.log &

