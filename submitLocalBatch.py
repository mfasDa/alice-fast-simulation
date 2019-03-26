#!/usr/bin/env python

import argparse
import logging
import os
import random
import shutil
import subprocess
import sys
import time
import yaml
from alifastsim import UserConfiguration as aliuserconfig
from alifastsim import PackageTools as alipackagetools
from alifastsim import GenerateHerwigInput as aliherwigtools
from alifastsim import GeneratePowhegInput as alipowhegtools
from alifastsim import Tools as alisimtools
from alifastsim import cernbatchtools as alicernsub
from alifastsim import nerscnatchtools as alinerscsub
from alifastsim import PackageTools as alipackagetools

repo = os.path.abspath(os.path.dirname(sys.argv[0]))

class submit_exception(Exception):

    def __init__(self):
        pass
    
    def __str__(self):
        return "Submitting job failed..."

def get_batchtools():
    if alinerscsub.is_nersc_system():
        return alinerscsub.nerscbatchtools()
    return alicernsub.cernbatchtools()

def SubmitParallel(LocalDest, ExeFile, Events, Jobs, yamlFileName, batchconfig, envscript):
    batchtools = get_batchtools()
    for ijob in range(0, Jobs):
        JobDir = LocalDest
        JobOutput = "{}/JobOutput_{:04d}.log".format(JobDir, ijob)
        RunJobFileName = "{}/RunJob_{:04d}.sh".format(JobDir, ijob)
        with open(RunJobFileName, "w") as myfile:
            myfile.write("#!/bin/bash\n")
            myfile.write(alipackagetools.GenerateComments())
            batchtools.get_batchhandler()(myfile, batchconfig, JobOutput)
            batchtools.writeSimCommand(repo, myfile, envscript, LocalDest, "{LocalDest}/{ExeFile} {yamlFileName} --numevents {Events} --job-number {ijob} --batch-job lbnl3\n".format(LocalDest=LocalDest, ExeFile=ExeFile, yamlFileName=os.path.basename(yamlFileName), Events=Events, ijob=ijob))
        output = alisimtools.subprocess_checkoutput([batchtools.get_batchsub(), RunJobFileName])
        print(output)

def SubmitParallelPowheg(LocalDest, ExeFile, Events, Jobs, yamlFileName, batchconfig, envscript, PowhegStage, XGridIter):
    batchtools = get_batchtools()
    input_file_name = alipowhegtools.GetParallelInputFileName(PowhegStage, XGridIter)
    shutil.copy("{}/{}".format(LocalDest, input_file_name), "{}/powheg.input".format(LocalDest))
    njobconfigStage = {1: 10, 2: 20, 3: 10, 4: Jobs} # Dictionary in stage:jobs

    for ijob in range(1, njobconfigStage[PowhegStage] + 1):
        JobDir = LocalDest
        if PowhegStage == 1:
            JobOutput = "{}/JobOutput_Stage_{}_XGridIter_{}_{:04d}.log".format(JobDir, PowhegStage, XGridIter, ijob)
            RunJobFileName = "{}/RunJob_Stage_{}_XGridIter_{}_{:04d}.sh".format(JobDir, PowhegStage, XGridIter, ijob)
        else:
            JobOutput = "{}/JobOutput_Stage_{}_{:04d}.log".format(JobDir, PowhegStage, ijob)
            RunJobFileName = "{}/RunJob_{}_{:04d}.sh".format(JobDir, PowhegStage, ijob)
        with open(RunJobFileName, "w") as myfile:
            myfile.write("#!/bin/bash\n")
            myfile.write(alipackagetools.GenerateComments())
            batchtools.get_batchhandler()(myfile, batchconfig, JobOutput)
            batchtools.writeSimCommand(repo, myfile, envscript, LocalDest, "{LocalDest}/{ExeFile} {LocalDest}/{yamlFileName} --numevents {Events} --job-number {ijob} --powheg-stage {PowhegStage} --batch-job lbnl3\n".format(LocalDest=LocalDest, ExeFile=ExeFile, yamlFileName=os.path.basename(yamlFileName), Events=Events, ijob=ijob, PowhegStage=PowhegStage))
        os.chmod(RunJobFileName, 0755)
        output = alisimtools.subprocess_checkoutput([batchtools.get_batchsub(), RunJobFileName])
        logging.info("%s", output)

def SubmitProcessingJobs(TrainName, LocalPath, Events, Jobs, Gen, Proc, yamlFileName, batchconfig, copy_files, PowhegStage, XGridIter, HerwigTune):
    logging.info("Submitting processing jobs for train {0}".format(TrainName))

    ExeFile = "runFastSim.py"
    LocalDest = "{0}/{1}".format(LocalPath, TrainName)

    envscript = "std_env.sh"
    if "powheg" in Gen:
        envscript = "powheg_env.sh"
    elif "herwig" in Gen:
        envscript = "herwig_env.sh"

    if copy_files:
        os.makedirs(LocalDest)
        FilesToCopy = {}
        FilesToDelete = []
        FilesToCopy["%s/%s" %(repo, yamlFileName)] = "%s/%s" %(LocalDest, os.path.basename(yamlFileName))
        FilesToCopy["%s/%s" %(repo, ExeFile)] = "%s/%s" %(LocalDest, ExeFile)
        Sourcefiles = ["OnTheFlySimulationGenerator.cxx", "OnTheFlySimulationGenerator.h",
                        "runJetSimulation.C", "start_simulation.C",
                        "lhapdf_utils.py",
                        "Makefile", "HepMC.tar",
                        "THepMCParser_dev.h", "THepMCParser_dev.cxx",
                        "AliGenExtFile_dev.h", "AliGenExtFile_dev.cxx",
                        "AliGenReaderHepMC_dev.h", "AliGenReaderHepMC_dev.cxx",
                        "AliGenEvtGen_dev.h", "AliGenEvtGen_dev.cxx",
                        "AliGenPythia_dev.h", "AliGenPythia_dev.cxx",
                        "AliPythia6_dev.h", "AliPythia6_dev.cxx",
                        "AliPythia8_dev.h", "AliPythia8_dev.cxx",
                        "AliPythiaBase_dev.h", "AliPythiaBase_dev.cxx"]

        if "pythia8" in Gen:
            FilesToCopy.append("powheg_pythia8_conf.cmnd")
        if "powheg" in Gen:
            alipowhegtools.main(yamlFileName, LocalDest, Events, 1, 1)
            alipowhegtools.main(yamlFileName, LocalDest, Events, 1, 2)
            alipowhegtools.main(yamlFileName, LocalDest, Events, 1, 3)
            alipowhegtools.main(yamlFileName, LocalDest, Events, 2)
            alipowhegtools.main(yamlFileName, LocalDest, Events, 3)
            alipowhegtools.main(yamlFileName, LocalDest, Events, 4)
            with open("{}/pwgseeds.dat".format(LocalDest), "w") as myfile:
                if Jobs > 20:
                    nseeds = Jobs
                else:
                    nseeds = 20
                for iseed in range(1, nseeds + 1):
                    rnd = random.randint(0, 1073741824)  # 2^30
                    myfile.write("{}\n".format(rnd))
        elif "herwig" in Gen:
            aliherwigtools.main(yamlFileName, "./", Events)
            Sourcefiles.extend(["herwig.in", "MB.in", "PPCollider.in", "SoftModel.in", "SoftTune.in"])
            if HerwigTune:
                Sourcefiles.append(HerwigTune)
            FilesToDelete.append("herwig.in")

        for f in Sourcefiles:
            FilesToCopy["%s/%s" %(repo, f)] = "%s/%s" %(LocalDest, f)

        alisimtools.copy_to_workdir(FilesToCopy)

        logging.info("Compiling analysis code...")
        get_batchtools().run_build(repo, LocalDest, envscript)
        for file in FilesToDelete: os.remove(file)

    if "powheg" in Gen:
        SubmitParallelPowheg(LocalDest, ExeFile, Events, Jobs, yamlFileName, batchconfig, envscript, PowhegStage, XGridIter)
    else:
        SubmitParallel(LocalDest, ExeFile, Events, Jobs, yamlFileName, batchconfig, envscript)


    logging.info("Done.")

def main(UserConf, yamlFileName, batchconfig, continue_powheg, powheg_stage, XGridIter):
    f = open(yamlFileName, 'r')
    config = yaml.load(f)
    f.close()
    
    Gen = config["gen"]
    Proc = config["proc"]
    HerwigTune = None
    if "herwig_config" in config and "tune" in config["herwig_config"]:
        HerwigTune = config["herwig_config"]["tune"]

    LocalPath = UserConf["local_path"]
    logging.info("Local working directory: %s", LocalPath)
    if not continue_powheg:
        unixTS = int(time.time())
        copy_files = True
        logging.info("New job with timestamp {0}".format(unixTS))
    else:
        unixTS = continue_powheg
        copy_files = False
        logging.info("Continue job with timestamp {0}".format(unixTS))
    TrainName = "FastSim_{0}_{1}_{2}".format(Gen, Proc, unixTS)
    try:
        SubmitProcessingJobs(TrainName, LocalPath, config["numevents"], config["numbjobs"], Gen, Proc, yamlFileName, batchconfig, copy_files, powheg_stage, XGridIter, HerwigTune)
    except submit_exception as e:
        logging.error("%s", e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local final merging for LEGO train results.')
    parser.add_argument('config', metavar='config.yaml', default="default.yaml", help='YAML configuration file')
    parser.add_argument('-u', '--user-conf', metavar='USERCONF', default="userConf.yaml")
    parser.add_argument('-b', '--batch-conf', metavar='BATCHCONFIG', default='', help="Batch job configuration")
    parser.add_argument('-d', '--debug', action = "store_true",  help = "Run with increased debug level")
    parser.add_argument('--continue-powheg', metavar='timestamp', default=None)
    parser.add_argument('--powheg-stage', type=int)
    parser.add_argument('--xgrid-iter', default=1, type=int)
    args = parser.parse_args()

    loglevel=logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(format='[%(levelname)s]: %(message)s', level=loglevel)
    if not len(args.batch_conf):
        logging.error("Batch config has to be specified")
        parser.print_help()
        sys.exit(2)

    userConf = aliuserconfig.LoadUserConfiguration(args.user_conf)

    main(userConf, args.config, args.batch_conf, args.continue_powheg, args.powheg_stage, args.xgrid_iter)
