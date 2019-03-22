#! /usr/bin/env python

import os
import subprocess
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
        scriptwriter.write("#SBATCH --qos=shared\n")
        scriptwriter.write("#SBATCH --output=%s\n" %outputfile)
        scriptwriter.write("#SBATCH --image=docker:mfasel/cc7-alice:latest\n")
        scriptwriter.write("#SBATCH --license=cscratch1,project\n") 
        scriptwriter.write("#SBATCH --time=%s\n" %batchconfig["time"])

    def get_batchhandler(self):
        return self.configbatch_slurm

    def get_batchsub(self):
        return "sbatch"

    def writeSimCommand(self, repo, scriptwriter, simcommand):
        scriptwriter.write("shifter %s/nersc/shifterrun.sh %s/powheg_herwig_env.sh \"%s\"\n" %(repo, os.environ["CSCRATCH"], simcommand))

    def run_build(self, repo, workdir):
        currentdir = os.getcwd()
        alisimtools.subprocess_cmd("shifter --image=docker:mfasel/cc7-alice:latest %s/nersc/shifterbuild.sh %s/powheg_herwig_env.sh %s" %(repo, os.environ["CSCRATCH"], workdir))
        os.chdir(currentdir)