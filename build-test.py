import yaml
import os
import time
import subprocess
import sys
import pathlib
import argparse

class scheduler():
  def __init__(self,scheduler_type):
     pass

  def submit(self):
     pass

  def create_header(self):
     pass

  def submit_job(self):
     pass

  def checkqueue(self):
     pass

class pbs(scheduler):
  def __init__(self,scheduler_type):
     self.type = scheduler_type

  def create_header(self,file_out,filename,queue,account,cpn,time):
    file_out.write("#!/bin/sh -l\n")
    file_out.write("#PBS -N {}\n".format(filename))
    file_out.write("#PBS -q {}\n".format(queue))
    file_out.write("#PBS -A {}\n".format(account))
    file_out.write("#PBS -l select=1:ncpus={}:mpiprocs={}\n".format(cpn,cpn))
    file_out.write("#PBS -l walltime={}\n".format(time))
    file_out.write("cd {}\n".format(os.getcwd()))

class slurm(scheduler):
  def __init__(self,scheduler_type):
     self.type = scheduler_type
     
  def create_header(self,file_out,filename,queue,account,partition,cluster,cpn,time):
    file_out.write("#!/bin/sh -l\n")
    file_out.write("#SBATCH -o {}_%j.o\n".format(filename))
    file_out.write("#SBATCH -e {}_%j.e\n".format(filename))
    file_out.write("#SBATCH --account={}\n".format(account))
    if(partition != "None"):
      file_out.write("#SBATCH --partition={}\n".format(partition))
    if(cluster != "None"):
      file_out.write("#SBATCH --cluster={}\n".format(cluster))
    file_out.write("#SBATCH --qos={}\n".format(queue))
    file_out.write("#SBATCH --nodes=1\n")
    file_out.write("#SBATCH --ntasks-per-node={}\n".format(cpn))
    file_out.write("#SBATCH --time={}\n".format(time))
    file_out.write("#SBATCH --exclusive\n")

class ESMFTest:
  def __init__(self, yaml_file, artifacts_root, workdir):
    self.yaml_file=yaml_file
    self.artifacts_root=artifacts_root
    self.workdir=workdir
    self.mypath=pathlib.Path(__file__).parent.absolute()
    self.branch="develop"
    self.readYAML()
    if(self.scheduler_type == "slurm"):
      self.scheduler=slurm("slurm")
    else:
      self.scheduler=slurm("pbs")
    print(self.yaml_file, self.artifacts_root, self.workdir)
    self.createJobCards()

  def readYAML(self):
    with open(self.yaml_file) as file:
      self.machine_list = yaml.load(file, Loader=yaml.FullLoader)
      self.machine_name = self.machine_list['machine']
      print("machine name is {}".format(self.machine_name))
      if("git-https" in self.machine_list):
        self.https = True
      else: 
        self.https = False
      self.account = self.machine_list['account']
      if("partition" in self.machine_list):
        self.partition = self.machine_list['partition']
      else: 
        self.partition = "None"
      self.account = self.machine_list['account']
      self.queue = self.machine_list['queue']
      self.cpn = self.machine_list['corespernode']
      self.scheduler_type = self.machine_list['scheduler']
      self.build_types = ['O','g']
      script_dir=os.getcwd()
      if("cluster" in self.machine_list):
        self.cluster=self.machine_list['cluster']
      else:
        self.cluster="None"
      if('build_time' in self.machine_list[self.comp]):
        self.build_time = self.machine_list[self.comp]['build_time']
      else:
        self.build_time = "1:00:00"
      if('test_time' in self.machine_list[self.comp]):
        self.test_time = self.machine_list[self.comp]['test_time']
      else:
        self.test_time = "1:00:00"

  def updateRepo(self):
     if(not(os.path.isdir(subdir))):
       if(self.https == True):
         cmdstring = "git clone -b {} https://github.com/esmf-org/esmf {}".format(self.branch,self.subdir)
       else:
         cmdstring = "git clone -b {} git@github.com:esmf-org/esmf {}".format(self.branch,self.subdir)
       status= subprocess.check_output(cmdstring,shell=True).strip().decode('utf-8')
       os.chdir(self.subdir)
       os.system("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
       os.system("git checkout {}".format(branch))
       os.system("git pull origin {}".format(branch))
     
  def createJobCards(self):
      for build_type in self.build_types:
        for comp in self.machine_list['compiler']:
         for ver in self.machine_list[comp]['versions']:
            print("{}".format(self.machine_list[comp]['versions'][ver]['mpi']))
            mpidict = self.machine_list[comp]['versions'][ver]['mpi']
            mpitypes= mpidict.keys()
            print(self.machine_list[comp]['versions'][ver])
            for key in mpitypes:
              self.subdir="{}_{}_{}_{}".format(comp,ver,key,build_type)
              updateRepo()
              filename = 'build-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
              t_filename = 'test-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
              fb = open(filename, "w")
              ft = open(t_filename, "w")
              create_header(fb,scheduler,filename,build_time,account,partition,queue,cpn,cluster)
              create_header(ft,scheduler,t_filename,test_time,account,partition,queue,cpn,cluster)
              if("unloadmodule" in machine_list[comp]):
                fb.write("\nmodule unload {}\n".format(machine_list[comp]['unloadmodule']))
                ft.write("\nmodule unload {}\n".format(machine_list[comp]['unloadmodule']))
              if("modulepath" in machine_list):
                modulepath = machine_list['modulepath']
                fb.write("\nmodule use {}\n".format(machine_list['modulepath']))
                ft.write("\nmodule use {}\n".format(machine_list['modulepath']))
              if("extramodule" in machine_list[comp]):
                fb.write("\nmodule load {}\n".format(machine_list[comp]['extramodule']))
                ft.write("\nmodule load {}\n".format(machine_list[comp]['extramodule']))
  
              if('extra_env_vars' in machine_list[comp]['versions'][ver]):
                  for var in machine_list[comp]['versions'][ver]['extra_env_vars']:
                    fb.write("export {}\n".format(machine_list[comp]['versions'][ver]['extra_env_vars'][var]))
                    ft.write("export {}\n".format(machine_list[comp]['versions'][ver]['extra_env_vars'][var]))
  
              if('extra_commands' in machine_list[comp]['versions'][ver]):
                  for cmd in machine_list[comp]['versions'][ver]['extra_commands']:
                    fb.write("{}\n".format(machine_list[comp]['versions'][ver]['extra_commands'][cmd]))
                    ft.write("{}\n".format(machine_list[comp]['versions'][ver]['extra_commands'][cmd]))

  def oldmain(self):
    with open(inpfile) as file:
      machine_list = yaml.load(file, Loader=yaml.FullLoader)
      machine_name = machine_list['machine']
      for build_type in build_types:
        for comp in self.machine_list['compiler']:
         for ver in self.machine_list[comp]['versions']:
              if(not(os.path.isdir(subdir))):
                 if(https == True):
  #                cmdstring = "git clone -b ESMF_8_1_0_beta_snapshot_43 https://github.com/esmf-org/esmf {}".format(subdir)
                   cmdstring = "git clone -b develop https://github.com/esmf-org/esmf {}".format(subdir)
                 else:
  #                cmdstring = "git clone -b ESMF_8_1_0_beta_snapshot_43 git@github.com:esmf-org/esmf {}".format(subdir)
                   cmdstring = "git clone -b develop git@github.com:esmf-org/esmf {}".format(subdir)
                 status= subprocess.check_output(cmdstring,shell=True).strip().decode('utf-8')
              os.chdir(subdir)
              os.system("rm -rf obj mod lib examples test")
              os.system("git checkout develop")
              filename = 'build-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
              t_filename = 'test-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
              fb = open(filename, "w")
              ft = open(t_filename, "w")
  
              create_header(fb,scheduler,filename,build_time,account,partition,queue,cpn,cluster)
  
    
  
              mpiflavor = mpidict[key]
              if(mpiflavor == "None"):
                mpiflavor = ""
    
              cmdstring = "export ESMF_DIR={}\n".format(os.getcwd())
              fb.write(cmdstring)
              ft.write(cmdstring)
    
              cmdstring = "export ESMF_COMPILER={}\n".format(comp)
              fb.write(cmdstring)
              ft.write(cmdstring)
    
              cmdstring="export ESMF_COMM={}\n".format(key)
              fb.write(cmdstring)
              ft.write(cmdstring)
    
              cmdstring="export ESMF_BOPT='{}'\n".format(build_type)
              fb.write(cmdstring)
              ft.write(cmdstring)
    
              cmdstring="export ESMF_TEST_EXHAUSTIVE='ON'\n"
              fb.write(cmdstring)
              ft.write(cmdstring)
    
              cmdstring="export ESMF_TEST_WITHTHREADS='ON'\n"
              fb.write(cmdstring)
              ft.write(cmdstring)
  
              if("mpi_env_vars" in mpidict[key]):
                for mpi_var in mpidict[key]['mpi_env_vars']:
                  fb.write("export {}\n".format(mpidict[key]['mpi_env_vars'][mpi_var]))
                  ft.write("export {}\n".format(mpidict[key]['mpi_env_vars'][mpi_var]))
              if(machine_list[comp]['versions'][ver]['netcdf'] == "None" ):
                modulecmd = "module load {} {} \nmodule list\n".format(machine_list[comp]['versions'][ver]['compiler'],mpiflavor['module'])
              else:
                modulecmd = "module load {} {} {}\nmodule list\n".format(machine_list[comp]['versions'][ver]['compiler'],mpiflavor['module'],machine_list[comp]['versions'][ver]['netcdf'])
              mpimodule = mpiflavor['module']
              if(mpimodule == "None"):
                mpiver = "None"
              else:
                mpiver = mpiflavor['module'].split('/')[-1]
              fb.write(modulecmd)
              ft.write(modulecmd)
              cmdstring = "make -j {}\n\n".format(cpn)
              fb.write(cmdstring)
              cmdstring = "make all_tests\n\n"
              ft.write(cmdstring)
    
              fb.close()
              ft.close()
              if(scheduler == "slurm"):
                batch_build = "sbatch {}".format(filename)
                print(batch_build)
                jobnum= subprocess.check_output(batch_build,shell=True).strip().decode('utf-8').split()[3]
                monitor_cmd = "python3 {}/get-results.py {} {} {} {} {} {} {}".format(mypath,jobnum,subdir,machine_name,scheduler,script_dir,artifacts_root,mpiver)
                proc = subprocess.Popen(monitor_cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)
                # submit the second job to be dependent on the first
                batch_test = "sbatch --depend=afterok:{} {}".format(jobnum,t_filename)
                print("Submitting test_batch with command: {}".format(batch_test))
                jobnum= subprocess.check_output(batch_test,shell=True).strip().decode('utf-8').split()[3]
                monitor_cmd = "python3 {}/get-results.py {} {} {} {} {} {} {}".format(mypath,jobnum,subdir,machine_name,scheduler,script_dir,artifacts_root,mpiver)
                proc = subprocess.Popen(monitor_cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)
              elif(scheduler == "pbs"):
                batch_build = "qsub {}".format(filename)
                print(batch_build)
                jobnum= subprocess.check_output(batch_build,shell=True).strip().decode('utf-8').split(".")[0]
                monitor_cmd = \
                     "python3 {}/get-results.py {} {} {} {} {} {} {}".format(mypath,jobnum,subdir,machine_name,scheduler,script_dir,artifacts_root,mpiver)
                print(monitor_cmd)
                proc = subprocess.Popen(monitor_cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)
                print("Submitting batch_build with command: {}, jobnum is {}".format(batch_build,jobnum))
                # submit the second job to be dependent on the first
                batch_test = "qsub -W depend=afterok:{} {}".format(jobnum,t_filename)
                print("Submitting test_batch with command: {}".format(batch_test))
                jobnum= subprocess.check_output(batch_test,shell=True).strip().decode('utf-8').split(".")[0]
                monitor_cmd = \
                     "python3 {}/get-results.py {} {} {} {} {} {} {}".format(mypath,jobnum,subdir,machine_name,scheduler,script_dir,artifacts_root,mpiver)
                print(monitor_cmd)
                proc = subprocess.Popen(monitor_cmd, shell=True)
              os.chdir("..")
    
    
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='ESMF nightly build/test system')
  parser.add_argument('-w','--workdir', help='directory where builds will be mad #', required=False,default=os.getcwd())
  parser.add_argument('-y','--yaml', help='Yaml file defining builds and testing parameters', required=True)
  parser.add_argument('-a','--artifacts', help='directory where artifacts will be placed', required=True)
  args = vars(parser.parse_args())
  
  test = ESMFTest(args['yaml'],args['artifacts'],args['workdir'])  
    
