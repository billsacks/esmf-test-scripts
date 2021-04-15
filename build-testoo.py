import yaml
import os
import time
import subprocess
import sys
import pathlib
import argparse

class scheduler():
  def __init__(self,scheduler_type,test):
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

  def create_header(self,headerType,test):
    if(headerType == "build"):
      file_out = test.fb
    else:
      file_out = test.ft
    if(test.dryrun):
      print("#!/bin/sh -l\n")
      if(headerType == "build"):
        print("#PBS -N {}".format(test.b_filename))
        print("#PBS -l walltime={}".format(test.build_time))
      else:
        print("#PBS -N {}".format(test.t_filename))
        print("#PBS -l walltime={}".format(test.test_time))
      print("#PBS -q {}".format(test.queue))
      print("#PBS -A {}".format(test.account))
      print("#PBS -l select=1:ncpus={}:mpiprocs={}".format(test.cpn,test.cpn))
      print("cd {}".format(os.getcwd()))
    else:
      file_out.write("#!/bin/sh -l\n")
      if(headerType == "build"):
        file_out.write("#PBS -N {}\n".format(test.b_filename))
        file_out.write("#PBS -l walltime={}\n".format(test.build_time))
      else:
        file_out.write("#PBS -N {}\n".format(test.t_filename))
        file_out.write("#PBS -l walltime={}\n".format(test.test_time))
      file_out.write("#PBS -q {}\n".format(test.queue))
      file_out.write("#PBS -A {}\n".format(test.account))
      file_out.write("#PBS -l select=1:ncpus={}:mpiprocs={}\n".format(test.cpn,test.cpn))
      file_out.write("cd {}\n".format(os.getcwd()))

class slurm(scheduler):
  def __init__(self,scheduler_type):
     self.type = scheduler_type

  def create_header(self,headerType,test):
    if(headerType == "build"):
      file_out = test.fb
    else:
      file_out = test.ft
    if(test.dryrun):
      print("#!/bin/sh -l")
      print("#SBATCH --account={}".format(test.account))
      if(headerType == "build"):
        print("#SBATCH -o {}_%j.o".format(test.b_filename))
        print("#SBATCH -e {}_%j.e".format(test.b_filename))
        print("#SBATCH --time={}".format(test.build_time))
      else:
        print("#SBATCH -o {}_%j.o".format(test.t_filename))
        print("#SBATCH -e {}_%j.e".format(test.t_filename))
        print("#SBATCH --time={}".format(test.test_time))
      if(test.partition != "None"):
        print("#SBATCH --partition={}".format(test.partition))
      if(test.cluster != "None"):
        print("#SBATCH --cluster={}".format(test.cluster))
      print("#SBATCH --qos={}".format(test.queue))
      print("#SBATCH --nodes=1")
      print("#SBATCH --ntasks-per-node={}".format(test.cpn))
      print("#SBATCH --exclusive")
    else:
      file_out.write("#!/bin/sh -l\n")
      file_out.write("#SBATCH --account={}\n".format(test.account))
      if(headerType == "build"):
        file_out.write("#SBATCH -o {}_%j.o\n".format(test.b_filename))
        file_out.write("#SBATCH -e {}_%j.e\n".format(test.b_filename))
        file_out.write("#SBATCH --time={}\n".format(test.build_time))
      else:
        file_out.write("#SBATCH -o {}_%j.o\n".format(test.t_filename))
        file_out.write("#SBATCH -e {}_%j.e\n".format(test.t_filename))
        file_out.write("#SBATCH --time={}\n".format(test.test_time))
      if(test.partition != "None"):
        file_out.write("#SBATCH --partition={}\n".format(test.partition))
      if(test.cluster != "None"):
        file_out.write("#SBATCH --cluster={}\n".format(test.cluster))
      file_out.write("#SBATCH --qos={}\n".format(test.queue))
      file_out.write("#SBATCH --nodes=1\n")
      file_out.write("#SBATCH --ntasks-per-node={}\n".format(test.cpn))
      file_out.write("#SBATCH --exclusive\n")

class ESMFTest:
  def __init__(self, yaml_file, artifacts_root, workdir, dryrun):
    self.yaml_file=yaml_file
    self.artifacts_root=artifacts_root
    self.workdir=workdir
    self.dryrun = dryrun
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
      if("account" in self.machine_list):
        self.account = self.machine_list['account']
      else: 
        self.account = "None"
      if("partition" in self.machine_list):
        self.partition = self.machine_list['partition']
      else: 
        self.partition = "None"
      if("queue" in self.machine_list):
        self.queue = self.machine_list['queue']
      else: 
        self.queue = "None"
      if("headnodename" in self.machine_list):
        self.headnodename = self.machine_list["headnodename"]
      else:
        self.headnodename = os.uname()[1]
      if("branch" in self.machine_list):
        self.branch = self.machine_list['branch']
      else: 
        self.branch = "develop"
      if("nuopcbranch" in self.machine_list):
        self.nuopcbranch = self.machine_list['nuopcbranch']
      else: 
        self.nuopcbranch = self.branch
      self.cpn = self.machine_list['corespernode']
      self.scheduler_type = self.machine_list['scheduler']
      self.build_types = ['O','g']
      script_dir=os.getcwd()
      if("cluster" in self.machine_list):
        self.cluster=self.machine_list['cluster']
      else:
        self.cluster="None"
      # Now traverse the tree
      for build_type in self.build_types:
        for comp in self.machine_list['compiler']:
  
         for ver in self.machine_list[comp]['versions']:
          mpidict = self.machine_list[comp]['versions'][ver]['mpi']
          mpitypes= mpidict.keys()
          print(self.machine_list[comp]['versions'][ver])
          for key in mpitypes:
            subdir="{}_{}_{}_{}".format(comp,ver,key,build_type)
            print("{}".format(subdir))
            if('build_time' in self.machine_list[comp]):
              self.build_time = self.machine_list[comp]['build_time']
            else:
              self.build_time = "1:00:00"
            if('test_time' in self.machine_list[comp]):
              self.test_time = self.machine_list[comp]['test_time']
            else:
              self.test_time = "1:00:00"

  def runcmd(self,cmd):
    if(self.dryrun):
       print("would have executed {}".format(cmd))
    else:
       os.system(cmd)
  def updateRepo(self,subdir):
     if(not(os.path.isdir(subdir))):
       if(self.https == True):
         cmdstring = "git clone -b {} https://github.com/esmf-org/esmf {}".format(self.branch,subdir)
       else:
         cmdstring = "git clone -b {} git@github.com:esmf-org/esmf {}".format(self.branch,subdir)
       if(self.dryrun):
         print("would have executed {}".format(cmdstring))
         print("would have cd'd to {}".format(subdir))
         os.system("mkdir {}".format(subdir))
       else:
         status= subprocess.check_output(cmdstring,shell=True).strip().decode('utf-8')
       os.chdir(subdir)
       self.runcmd("rm -rf obj mod lib examples test *.o *.e *bat.o* *bat.e*")
       self.runcmd("git checkout {}".format(self.branch))
       self.runcmd("git pull origin {}".format(self.branch))
     
  def createJobCards(self):
      for build_type in self.build_types:
        for comp in self.machine_list['compiler']:
         for ver in self.machine_list[comp]['versions']:
            print("{}".format(self.machine_list[comp]['versions'][ver]['mpi']))
            mpidict = self.machine_list[comp]['versions'][ver]['mpi']
            mpitypes= mpidict.keys()
            print(self.machine_list[comp]['versions'][ver])
            for branch in self.machine_list['branch']:
              for key in mpitypes:
                subdir="{}_{}_{}_{}".format(comp,ver,key,build_type)
                self.updateRepo(subdir)
                self.b_filename = 'build-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
                self.t_filename = 'test-{}_{}_{}_{}.bat'.format(comp,ver,key,build_type)
                self.fb = open(self.b_filename, "w")
                self.ft = open(self.t_filename, "w")
                self.scheduler.create_header("build",self)
                self.scheduler.create_header("test",self)
#              if("unloadmodule" in machine_list[comp]):
#                fb.write("\nmodule unload {}\n".format(machine_list[comp]['unloadmodule']))
#                ft.write("\nmodule unload {}\n".format(machine_list[comp]['unloadmodule']))
#              if("modulepath" in machine_list):
#                modulepath = machine_list['modulepath']
#                fb.write("\nmodule use {}\n".format(machine_list['modulepath']))
#                ft.write("\nmodule use {}\n".format(machine_list['modulepath']))
#              if("extramodule" in machine_list[comp]):
#                fb.write("\nmodule load {}\n".format(machine_list[comp]['extramodule']))
#                ft.write("\nmodule load {}\n".format(machine_list[comp]['extramodule']))
#  
#              if('extra_env_vars' in machine_list[comp]['versions'][ver]):
#                  for var in machine_list[comp]['versions'][ver]['extra_env_vars']:
#                    fb.write("export {}\n".format(machine_list[comp]['versions'][ver]['extra_env_vars'][var]))
#                    ft.write("export {}\n".format(machine_list[comp]['versions'][ver]['extra_env_vars'][var]))
#  
#              if('extra_commands' in machine_list[comp]['versions'][ver]):
#                  for cmd in machine_list[comp]['versions'][ver]['extra_commands']:
#                    fb.write("{}\n".format(machine_list[comp]['versions'][ver]['extra_commands'][cmd]))
#                    ft.write("{}\n".format(machine_list[comp]['versions'][ver]['extra_commands'][cmd]))

        os.chdir("..")
    
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='ESMF nightly build/test system')
  parser.add_argument('-w','--workdir', help='directory where builds will be mad #', required=False,default=os.getcwd())
  parser.add_argument('-y','--yaml', help='Yaml file defining builds and testing parameters', required=True)
  parser.add_argument('-a','--artifacts', help='directory where artifacts will be placed', required=True)
  parser.add_argument('-d','--dryrun', help='directory where artifacts will be placed', required=False,default=False)
  args = vars(parser.parse_args())
  
  test = ESMFTest(args['yaml'],args['artifacts'],args['workdir'],args['dryrun'])  
    
