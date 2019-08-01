#! /usr/bin/env python3
import os
import subprocess
import logging
from alifastsim import Tools as alisimtools
from alifastsim import PackageTools as alipackagetools

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

    def submitJobs(self, repo, simtask, workdir, jobscriptbase, logfilebase, envscript, batchconfig, njobs, joboffset):
        for ijob in range(joboffset, njobs + joboffset):
            taskjobscriptname = jobscriptbase
            taskjobscriptname = os.path.join(workdir, taskjobscriptname.replace("RANK", "%04d" %ijob))
            tasklogfile = logfilebase
            tasklogfile = os.path.join(workdir, tasklogfile.replace("RANK", "%04d" %ijob))
            logging.info("Using jobscript {jobscript}, writing to {logfile}".format(jobscript=taskjobscriptname,logfile=tasklogfile))
            with open(taskjobscriptname, 'w') as jobscriptwriter:
                jobscriptwriter.write("#!/bin/bash\n")
                jobscriptwriter.write(alipackagetools.GenerateComments())
                self.get_batchhandler()(jobscriptwriter, batchconfig, tasklogfile)
                self.writeSimCommand(repo, jobscriptwriter, envscript, workdir, simtask.create_task_command_serial(ijob))
                jobscriptwriter.write("cd %s\n" %workdir)
                jobscriptwriter.write("echo WD: $PWD\n")
                self.writeCleanCommand(jobscriptwriter, envscript, ijob)
                jobscriptwriter.close()
                os.chmod(taskjobscriptname, 0o755)
                output = alisimtools.subprocess_checkoutput([self.get_batchsub(), taskjobscriptname])
                logging.info("%s", output)

    def get_batchhandler(self):
        return self.configbatch_slurm if alisimtools.test_slurm() else self.configbatch_pbs
   
    def get_batchsub(self):
        return "sbatch" if alisimtools.test_slurm() else "qsub"

    def writeSimCommand(self, repo, scriptwriter, envscript, workdir, simcommand):
        scriptwriter.write("source $HOME/%s\n" %envscript)
        scriptwriter.write("%s\n" %simcommand)

    def writeCleanCommand(self, jobscriptwriter, envscript, jobid):
        FilesToDelete = []
        if "herwig" in envscript:
            FilesToDelete.append("events_%04d.hepmc" %jobid)
            FilesToDelete.append("herwig_%04d.in" %jobid)
            FilesToDelete.append("herwig_%04d.run" %jobid)
        elif "powheg" in envscript:
            FilesToDelete.append("pwgevents-%04d.lhe" %jobid)
            FilesToDelete.append("pwgevents-%04d.lhe.bak" %jobid)
        for f in FilesToDelete:
            jobscriptwriter.write("rm -v %s\n" %f)

    def run_build(self, repo, workdir, envscript):
        currentdir = os.getcwd()
        subprocess.call("%s/nersc/shifterbuild.sh %s/%s %s" %(repo,os.environ["HOME"],envscript,workdir), shell=True)
        os.chdir(currentdir)
