#! /usr/bin/env python
import os
import subprocess
from alifastsim import Tools as alisimtools

class cernbatchtools:

    class cern_buildexception(Exception):

        def __init__(self):
            pass

        def __str__(self):
            return "Environment is not configured correctly!"

    def configbatch_slurm(self, scriptwriter, batchconfig, outputfile):
        scriptwriter.write("#SBATCH --output=%s\n" %outputfile)
        scriptwriter.write("#SBATCH -N 1\n")
        scriptwriter.write("#SBATCH -n 1\n")
        scriptwriter.write("#SBATCH -c 1\n")

    def configbatch_pbs(self, scriptwriter, batchconfig, outputfile):
        scriptwriter.write("#PBS -o %s\n" %outputfile)
        scriptwriter.write("#PBS -j oe\n")

    def get_batchhandler(self):
        return self.configbatch_slurm if alisimtools.test_slurm() else self.configbatch_pbs
   
    def get_batchsub(self):
        return "sbatch" if alisimtools.test_slurm() else "qsub"

    def writeSimCommand(self, repo, scriptwriter, simcommand):
        scriptwriter.write("source $HOME/powheg_herwig_env.sh\n")
        scriptwriter.write("%s\n" %simcommand)

    def run_build(self, repo, workdir):
        currentdir = os.getcwd()
        subprocess.call("%s/nersc/shifterbuild.sh %s/powheg_herwig_env.sh %s" %(repo,os.environ["HOME"],workdir), shell=True);
        os.chdir(currentdir)
