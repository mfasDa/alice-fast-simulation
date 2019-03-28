#! /usr/bin/env python

import os
import math
import logging
import subprocess
import yaml
from alifastsim import Tools as alisimtools
from alifastsim import PackageTools as alipackagetools

def is_nersc_system():
    nersc_host = False
    for k in os.environ.keys():
        if "NERSC" in k:
            nersc_host = True
            break
    return nersc_host

class nerscbatchtools:

    class cern_buildexception(Exception):

        def __init__(self):
            pass

        def __str__(self):
            return "Environment is not configured correctly!"

    def configbatch_slurm(self, scriptwriter, batchconfig, nnodes, ntasks, ncpu, outputfile):
        breader = open(batchconfig, "r")
        bcdata = yaml.load(breader)
        breader.close()
        nerscsystem = os.environ["NERSC_HOST"]
        scriptwriter.write("#SBATCH --qos=%s\n" %bcdata["qos"])
        taskspernode={"edison":24, "cori": 68}
        if bcdata["qos"] != "shared":
                scriptwriter.write("#SBATCH --nodes=%d\n", nnodes)
                scriptwriter.write("#SBATCH --tasks-per-node=%d\n" %taskspernode[nerscsystem])
                if nerscsystem == "cori":
                    scriptwriter.write("#SBATCH --constraint=knl")
        else:
                scriptwriter.write("#SBATCH --ntasks=1\n", ntasks)
                scriptwriter.write("#SBATCH --cpus-per-task=1\n", ncpu)
        scriptwriter.write("#SBATCH --output=%s\n" %outputfile)
        scriptwriter.write("#SBATCH --image=docker:mfasel/cc7-alice:latest\n")
        scriptwriter.write("#SBATCH --license=cscratch1,project\n") 
        scriptwriter.write("#SBATCH --time=%s\n" %bcdata["time"])

    def submitJobs(self, repo, simtask, workdir, jobscriptbase, logfilebase, envscript, batchconfig, njobs, joboffset):
        breader = open(batchconfig, "r")
        bcdata = yaml.load(breader)
        breader.close()
        
        ismpiqueue = bcdata["qos"] != "shared"
        nerscsystem = os.environ["NERSC_HOST"]
        if ismpiqueue:
            #determine nodes and number of CPUs
            taskspernode={"edison":24, "cori": 68}
            nnodes = int(math.ceil(float(njobs)/float(taskspernode[nerscsystem])))
            taskjobscriptname = jobscriptbase
            taskjobscriptname = os.path.join(workdir, taskjobscriptname.replace("RANK", "MPI"))
            generallogfile = logfilebase
            generallogfile = os.path.join(workdir, generallogfile.replace("RANK", "ALL"))
            #submit one single mpi job
            with open(os.path.join(workdir, taskjobscriptname)) as jobscriptwriter:
                jobscriptwriter.writer("#!/bin/bash\n")
                jobscriptwriter.write(alipackagetools.GenerateComments())
                self.configbatch_slurm(jobscriptwriter, batchconfig, nnodes, 0, 0, generallogfile)
                self.writeSimCommandMPI(repo, jobscriptwriter, njobs, joboffset, envscript, workdir, simtask.create_task_command_mpi(), logfilebase)
                jobscriptwriter.close()
                os.chmod(os.path.join(workdir, taskjobscriptname), 0755)
                output = alisimtools.subprocess_checkoutput([self.get_batchsub(), taskjobscriptname])
                logging.info("%s", output)

        else:
            #submit multiple serial jobs
            for ijob in range(joboffset, njobs + joboffset):
                taskjobscriptname = jobscriptbase
                taskjobscriptname = taskjobscriptname.replace("RANK", "%0d" %ijob)
                tasklogfile = logfilebase
                tasklogfile = tasklogfile.replace("RANK", "%04d" %ijob)
                with open(os.path.join(workdir, taskjobscriptname)) as jobscriptwriter:
                    jobscriptwriter.writer("#!/bin/bash\n")
                    jobscriptwriter.write(alipackagetools.GenerateComments())
                    self.configbatch_slurm(jobscriptwriter, batchconfig, 1, 1, 1, tasklogfile)
                    self.writeSimCommand(self, repo, jobscriptwriter, envscript, workdir, simtask.create_task_command_serial(ijob))
                    jobscriptwriter.close()
                    os.chmod(os.path.join(workdir, taskjobscriptname), 0755)
                    output = alisimtools.subprocess_checkoutput([self.get_batchsub(), taskjobscriptname])
                    logging.info("%s", output)
                    

    def get_batchhandler(self):
        return self.configbatch_slurm

    def get_batchsub(self):
        return "sbatch"

    def writeSimCommand(self, repo, scriptwriter, envscript, workdir, simcommand):
        scriptwriter.write("shifter %s/nersc/shifterrun.sh %s/%s %s \"%s\"\n" %(repo, os.environ["CSCRATCH"], envscript, workdir, simcommand))

    def writeSimCommandMPI(self, repo, scriptwriter, njobs, joboffset, envscript, workdir, simcommand, logfiletemplate):
        scriptwriter.write("srun shifter %s/nersc/mpiwrapper.py %s/%s %d %d %s \"%s\" %s\n" %(repo, os.environ["CSCRATCH"], njobs, joboffset, envscript, workdir, simcommand, logfiletemplate))

    def run_build(self, repo, workdir, envscript):
        currentdir = os.getcwd()
        subprocess.call("shifter --image=docker:mfasel/cc7-alice:latest %s/nersc/shifterbuild.sh %s/%s %s" %(repo, os.environ["CSCRATCH"], envscript, workdir), shell=True)
        os.chdir(currentdir)
