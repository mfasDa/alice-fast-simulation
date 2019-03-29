#! /usr/bin/env python
from mpi4py import MPI
import logging
import os
import subprocess
import sys

repo = os.path.dirname(sys.argv[0])

def run_local_job(workdir, envscript, task, logfile):
    logging.info("Running \"%s\", logging to %s", task, logfile)
    subprocess.call("%s/shifterrun.sh %s %s \"%s\" &> %s" %(repo, envscript, workdir, task, logfile), shell=True)

def adapt_jobid(content, jobid):
    result = content
    return result.replace("RANK", "%04d" %jobid)

if __name__ == "__main__":
    njobs = sys.argv[1]
    if MPI.COMM_WORLD.Get_rank() >= njobs:
        sys.exit(0)     # More CPUs than jobs due to whole 
    logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)
    logging.info("Starting worker %d ...", MPI.COMM_WORLD.Get_rank())
    taskoffset = sys.argv[2]
    envscript = sys.argv[3]
    workdir = sys.argv[4]
    task = sys.argv[5]
    logfiletemplate = sys.argv[6]
    workertask = adapt_jobid(task, MPI.COMM_WORLD.Get_rank())
    logfile = adapt_jobid(logfiletemplate, MPI.COMM_WORLD.Get_rank())
    run_local_job(workdir, envscript, workertask, logfile)
    logging.info("Worker %d done", MPI.COMM_WORLD.Get_rank())
