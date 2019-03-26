#! /usr/bin/env python

import os
import subprocess
import yaml
from alifastsim import Tools as alisimtools

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

    def configbatch_slurm(self, scriptwriter, batchconfig, outputfile):
        breader = open(batchconfig, "r")
        bcdata = yaml.load(breader)
        breader.close()
        scriptwriter.write("#SBATCH --qos=%s\n" %bcdata["qos"])
        if bcdata["qos"] != "shared":
                scriptwriter.write("#SBATCH --nodes=1\n")
        else:
                scriptwriter.write("#SBATCH --ntasks=1\n")
                scriptwriter.write("#SBATCH --cpus-per-task=1\n")
        scriptwriter.write("#SBATCH --output=%s\n" %outputfile)
        scriptwriter.write("#SBATCH --image=docker:mfasel/cc7-alice:latest\n")
        scriptwriter.write("#SBATCH --license=cscratch1,project\n") 
        scriptwriter.write("#SBATCH --time=%s\n" %bcdata["time"])

    def get_batchhandler(self):
        return self.configbatch_slurm

    def get_batchsub(self):
        return "sbatch"

    def writeSimCommand(self, repo, scriptwriter, envscript, workdir, simcommand):
        scriptwriter.write("shifter %s/nersc/shifterrun.sh %s/%s %s \"%s\"\n" %(repo, os.environ["CSCRATCH"], envscript, workdir, simcommand))

    def run_build(self, repo, workdir, envscript):
        currentdir = os.getcwd()
        subprocess.call("shifter --image=docker:mfasel/cc7-alice:latest %s/nersc/shifterbuild.sh %s/%s %s" %(repo, os.environ["CSCRATCH"], envscript, workdir), shell=True)
        os.chdir(currentdir)
