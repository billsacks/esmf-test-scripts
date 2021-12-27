import os
import subprocess
import datetime
import sys
import time
import glob
import re
import pathlib


def checkqueue(jobid, scheduler):
    if scheduler == "slurm":
        queue_query = f"sacct -j {jobid} | head -n 3 | tail -n 1 | awk -F ' ' '{{print $6}}'"
    elif scheduler == "pbs":
        queue_query = f"qstat -H {jobid} | tail -n 1 | awk -F ' +' '{{print $10}}'"
    elif scheduler == "None":
        return True
    else:
        sys.exit("unsupported job scheduler")
    try:
        result = subprocess.check_output(queue_query, shell=True).strip().decode("utf-8")
        if scheduler == "pbs":
            if result == "F":  # could check for R and Q to see if it is running or waiting
                return True
            else:
                return False
        if scheduler == "slurm":
            if (
                (result == "COMPLETED")
                or (result == "TIMEOUT")
                or (result == "FAILED")
                or (result == "CANCELLED")
            ):  # could check for R and Q to see if it is running or waiting
                return True
            else:
                return False
    except:
        result = "done"
        return True
    return False


def copy_artifacts(
    build_dir,
    artifacts_root,
    machine_name,
    mpiversion,
    oe_filelist,
    jobid,
    scheduler,
    branch,
):

    build_basename = os.path.basename(build_dir)
    gitbranch = branch
    dirbranch = re.sub("/", "_", branch)
    print(f"build_basename is {build_basename}")
    parts = build_basename.split("_")
    # [compiler, version, mpiflavor, build_type,dirbranch] = build_basename.split("_")
    compiler = parts[0]
    version = parts[1]
    mpiflavor = parts[2]
    build_type = parts[3]
    # get the full path for placment of artifacts
    if mpiversion != "None":
        outpath = "{}/{}/{}/{}/{}/{}/{}/{}".format(
            artifacts_root,
            dirbranch,
            machine_name,
            compiler,
            version,
            build_type,
            mpiflavor,
            mpiversion,
        )
    else:
        outpath = "{}/{}/{}/{}/{}/{}/{}".format(
            artifacts_root,
            dirbranch,
            machine_name,
            compiler,
            version,
            build_type,
            mpiflavor,
        )
    # copy/rename the stdout/stderr files to artifacts out directory
    test_stage = False
    print(f"outpath is {outpath}")
    for cfile in oe_filelist:
        print(f"cfile is {cfile}")
        if (
            cfile.find(f"test_{jobid}") != -1
        ):  # this is just the build job, so no test artifacts yet
            test_stage = True
    if not test_stage:
        # remove old files in out directory
        print("just the build stage, so remove old files")
        cmd = "mkdir -p {}/out; rm {}/*/*; rm {}/*.log; rm {}/summary.dat".format(
            outpath, outpath, outpath, outpath
        )
        print(f"cmd is {cmd}\n")
        os.system(cmd)
    # print("oe filelist is {}".format(oe_filelist))
    if oe_filelist == []:
        return
    for cfile in oe_filelist:
        nfile = os.path.basename(re.sub(f"_{jobid}", "", cfile))
        #   print("cfile is {}, and find says {} ".format(cfile,cfile.find('test_{}'.format(jobid))))
        cp_cmd = f"cp {cfile} {outpath}/out/{nfile}"
        print(f"cp command is {cp_cmd}")
        os.system(cp_cmd)
    if not (test_stage):
        git_cmd = "cd {};git checkout {};git add {}/{};git commit -a -m'update for build {} on {} [ci skip]';git push origin {}".format(
            artifacts_root,
            machine_name,
            dirbranch,
            machine_name,
            build_basename,
            machine_name,
            machine_name,
        )
        print(f"git_cmd is {git_cmd}")
        os.system(git_cmd)
        return
    # Make directories, if they aren't already there
    cmd = f"mkdir -p {outpath}/examples; rm {outpath}/examples/*; rm {outpath}/*"
    os.system(cmd)
    cmd = f"mkdir -p {outpath}/apps; rm {outpath}/apps/*"
    os.system(cmd)
    cmd = f"mkdir -p {outpath}/test; rm {outpath}/test/*"
    os.system(cmd)
    cmd = f"mkdir -p {outpath}/lib; rm {outpath}/lib/*"
    os.system(cmd)
    print("globbing examples")
    example_artifacts = glob.glob(f"{build_dir}/examples/examples{build_type}/*/*.Log")
    example_artifacts.extend(glob.glob(f"{build_dir}/examples/examples{build_type}/*/*.stdout"))
    # get information from example results file to accumulate
    ex_result_file = glob.glob(f"{build_dir}/examples/examples{build_type}/*/*results")
    if len(ex_result_file) > 0:
        example_results = (
            subprocess.check_output(f"cat {ex_result_file[0]}", shell=True).strip().decode("utf-8")
        )
    else:
        example_results = "No examples ran"
    # get information from test results files to accumulate
    test_artifacts = glob.glob(f"{build_dir}/test/test{build_type}/*/*.Log")
    print("test_artifacts are ".format(test_artifacts))
    test_artifacts.extend(glob.glob(f"{build_dir}/test/test{build_type}/*/*.stdout"))
    try:
        unit_results = (
            subprocess.check_output(
                f"cat {build_dir}/test/test{build_type}/*/unit_tests_results",
                shell=True,
            )
            .strip()
            .decode("utf-8")
        )
    except:
        unit_results = "unit tests did not complete"
    try:
        system_results = (
            subprocess.check_output(
                f"cat {build_dir}/test/test{build_type}/*/system_tests_results",
                shell=True,
            )
            .strip()
            .decode("utf-8")
        )
    except:
        system_results = "system tests did not complete"

    python_artifacts = glob.glob(f"{build_dir}/src/addon/ESMPy/*.log")

    cwd = os.getcwd()
    os.chdir(build_dir)
    build_hash = subprocess.check_output("git describe --tags", shell=True).strip().decode("utf-8")
    make_info = (
        subprocess.check_output("cat module-build.log; cat info.log", shell=True)
        .strip()
        .decode("utf-8")
    )
    os.chdir(cwd)
    esmfmkfile = glob.glob(f"{build_dir}/lib/lib{build_type}/*/esmf.mk")
    print(f"esmfmkfile is {esmfmkfile}")
    build_time = datetime.datetime.fromtimestamp(os.path.getmtime(esmfmkfile[0]))
    summary_file = open(f"{outpath}/summary.dat", "w")
    summary_file.write("\n===================================================================\n")
    summary_file.write(
        f"Build for = {build_basename}, mpi version {mpiversion} on {machine_name}\n"
    )
    summary_file.write(f"Build time = {build_time}\n")
    summary_file.write(f"git hash = {build_hash}\n\n")
    unit_results = re.sub(" FAIL", "\tFAIL", unit_results)
    system_results = re.sub(" FAIL", " \tFAIL", system_results)
    example_results = re.sub(" FAIL", " \tFAIL", example_results)
    summary_file.write(f"unit test results   \t{unit_results}\n")
    summary_file.write(f"system test results \t{system_results}\n")
    summary_file.write(f"example test results \t{example_results}\n\n")
    summary_file.write("\n===================================================================\n")
    summary_file.write(f"\n\n{make_info}\n\n")
    summary_file.write("\n===================================================================\n")
    summary_file.close()
    # return
    timestamp = f"build time -- {build_time}"
    for afile in example_artifacts:
        cmd = f"echo {timestamp} > {outpath}/examples/{os.path.basename(afile)}"
        os.system(cmd)
        cmd = f"cat {afile} >> {outpath}/examples/{os.path.basename(afile)}"
        #   cmd = 'cp {} {}/examples'.format(afile,outpath)
        print(f"cmd is {cmd}")
        os.system(cmd)
    for afile in test_artifacts:
        cmd = f"echo {timestamp} > {outpath}/test/{os.path.basename(afile)}"
        os.system(cmd)
        cmd = f"cat {afile} >> {outpath}/test/{os.path.basename(afile)}"
        #   cmd = 'cp {} {}/test".format(afile,outpath)
        print(f"cmd is {cmd}")
        os.system(cmd)
    for afile in esmfmkfile:
        cmd = f"echo {timestamp} > {outpath}/lib/{os.path.basename(afile)}"
        os.system(cmd)
        cmd = f"cat {afile} >> {outpath}/lib/{os.path.basename(afile)}"
        #   cmd = 'cp {} {}/lib'.format(afile,outpath)
        print(f"cmd is {cmd}")
        os.system(cmd)
    for afile in python_artifacts:
        cmd = f"echo {timestamp} > {outpath}/{os.path.basename(afile)}"
        os.system(cmd)
        cmd = f"cat {afile} >> {outpath}/{os.path.basename(afile)}"
        #   cmd = 'cp {} {}'.format(afile,outpath)
        print(f"cmd is {cmd}")
        os.system(cmd)

    git_cmd = "cd {};git checkout {};git add {}/{};git commit -a -m'update for test {} on {} [ci skip]';git push origin {}".format(
        artifacts_root,
        machine_name,
        dirbranch,
        machine_name,
        build_basename,
        machine_name,
        machine_name,
    )
    # print("git_cmd is {}".format(git_cmd))
    os.system(git_cmd)
    return


def main(argv):
    root_path = pathlib.Path(__file__).parent.absolute()
    jobid = sys.argv[1]
    build_basename = sys.argv[2]
    machine_name = sys.argv[3]
    scheduler = sys.argv[4]
    test_root_dir = sys.argv[5]
    artifacts_root = sys.argv[6]
    mpiver = sys.argv[7]
    branch = sys.argv[8]
    start_time = time.time()
    seconds = 14400
    build_dir = f"{test_root_dir}/{build_basename}"
    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        job_done = checkqueue(jobid, scheduler)
        if job_done:
            #     oe_filelist = glob.glob('{}/{}/*{}*'.format(test_root_dir,build_basename,jobid))
            oe_filelist = glob.glob(f"{test_root_dir}/{build_basename}/*_{jobid}*.log")
            oe_filelist.extend(glob.glob(f"{test_root_dir}/{build_basename}/*.bat"))
            oe_filelist.extend(glob.glob(f"{test_root_dir}/{build_basename}/module-*.log"))
            print(f"filelist is {oe_filelist}")
            print(f"oe list is {oe_filelist}\n")
            copy_artifacts(
                build_dir,
                artifacts_root,
                machine_name,
                mpiver,
                oe_filelist,
                jobid,
                scheduler,
                branch,
            )
            break
        time.sleep(30)

        if elapsed_time > seconds:
            print("Finished iterating in: " + str(int(elapsed_time)) + " seconds")
            break


if __name__ == "__main__":
    main(sys.argv[1:])
