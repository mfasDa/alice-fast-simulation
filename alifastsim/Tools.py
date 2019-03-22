#! /usr/bin/env python
import logging
import shutil
import subprocess

def test_slurm():
    try:
        sbatchpath = subprocess.check_output(["which", "sbatch"]).rstrip()
    except subprocess.CalledProcessError:
        logging.error("Slurm executable '{}' not found!".format("sbatch"))
        return False
    logging.debug("Slurm found in '{}'".format(sbatchpath))
    return True

def subprocess_call(cmd):
    logging.debug(cmd)
    return subprocess.call(cmd)

def subprocess_checkcall(cmd):
    logging.debug(cmd)
    return subprocess.check_call(cmd)

def subprocess_checkoutput(cmd):
    logging.debug(cmd)
    return subprocess.check_output(cmd, universal_newlines=True)

def subprocess_cmd(command):
    process = subprocess.Popen("/bin/bash", stdout=subprocess.PIPE)
    proc_stdout = process.communicate(command)[0].strip()
    logging.info(proc_stdout)

def copy_to_workdir(Files):
    for inputfile, outputfile in Files.iteritems():
        logging.info("Copying %s to %s", inputfile, outputfile)
        shutil.copy(inputfile, outputfile)
