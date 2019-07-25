#!/usr/bin/env python3

# script to submit fast simulation jobs to the grid
# submit a processing job using POWHEG charm settings with 100 subjobs, each producing 50k events
# ./submit_grid.py --aliphysics vAN-20161101-1 --gen powheg --proc charm --numevents 50000 --numjobs 100
# submit a merging job with a maximum of 15 files processed in each subjob (you will need the timestamp number provided after submitting the processing job)
# ./submit_grid.py --aliphysics vAN-20161101-1 --gen powheg --proc charm --merge [TIMESTAMP] --max-files-per-job 15
# you should keep submitting merging jobs until everything is merged in an acceptable number of files (you can go on until everything is merged in a single file if you like)
# download your results from the last merging stage (you will need the timestamp number provided after submitting the processing job)
# ./submit_grid.py --aliphysics vAN-20161101-1 --gen powheg --proc charm --download [TIMESTAMP]
# you can specify a merging stage if you want to download an intermediate merging stage using the option "--stage [STAGE]"

import argparse
import datetime
import glob
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import time
import yaml
from alifastsim import UserConfiguration as aliuserconfig
from alifastsim import GeneratePowhegInput as alipowhegtools
from alifastsim import GenerateHerwigInput as aliherwigtools
from alifastsim import Tools as alisimtools
from alifastsim import GridTools as aligridtools
from alifastsim import PackageTools as alipackagetools


def GenerateProcessingJDL(Exe, AlienDest, Packages, ValidationScript, FilesToCopy, TTL, Events, Jobs, yamlFileName, MinPtHard, MaxPtHard, PowhegStage):
    comments = alipackagetools.GenerateComments()
    jdlContent = "{comments} \n\
Executable = \"{dest}/{executable}\"; \n\
# Time after which the job is killed (120 min.) \n\
TTL = \"{TTL}\"; \n\
OutputDir = \"{dest}/output/#alien_counter_03i#\"; \n\
Output = {{ \n\
\"log_archive.zip:stderr,stdout,*.log@disk=1\", \n\
\"root_archive.zip:AnalysisResults*.root@disk=2\" \n\
}}; \n\
Arguments = \"{yamlFileName} --numevents {Events} --minpthard {MinPtHard} --maxpthard {MaxPtHard} --batch-job grid --job-number #alien_counter# --powheg-stage {PowhegStage}\"; \n\
".format(yamlFileName=yamlFileName, MinPtHard=MinPtHard, MaxPtHard=MaxPtHard, comments=comments, executable=Exe, dest=AlienDest, Packages=Packages, Events=Events, TTL=TTL, PowhegStage=PowhegStage)

    if Packages:
        jdlContent += "Packages = {{ \n\
{Packages} \
}};\n".format(Packages=Packages)

    jdlContent += "Split=\"production:1-{Jobs}\"; \n\
ValidationCommand = \"{dest}/{validationScript}\"; \n\
# List of input files to be uploaded to workers \n\
".format(dest=AlienDest, validationScript=ValidationScript, Jobs=Jobs)

    if len(FilesToCopy) > 0:
        jdlContent += "InputFile = {"
        start = True
        for myfile in FilesToCopy:
            if start:
                jdlContent += "\n"
            else:
                jdlContent += ", \n"
            start = False
            jdlContent += "\"LF:{dest}/{f}\"".format(dest=AlienDest, f=os.path.basename(myfile))
        jdlContent += "}; \n"

    return jdlContent

def GenerateXMLCollection(Path, XmlName):
    return alisimtools.subprocess_checkoutput(["alien_find", "-x", XmlName, Path, "*/AnalysisResults*.root"])

def GenerateMergingJDL(Exe, Xml, AlienDest, TrainName, AliPhysicsVersion, ValidationScript, FilesToCopy, TTL, MaxFilesPerJob, SplitMethod):
    comments = alipackagetools.GenerateComments()
    jdlContent = "{comments} \n\
Executable = \"{dest}/{executable}\"; \n\
# Time after which the job is killed (120 min.) \n\
TTL = \"{TTL}\"; \n\
OutputDir = \"{dest}/output/#alien_counter_03i#\"; \n\
Output = {{ \n\
\"log_archive.zip:stderr,stdout,*.log@disk=1\", \n\
\"root_archive.zip:AnalysisResults*.root@disk=2\" \n\
}}; \n\
Arguments = \"{trainName} --xml wn.xml\"; \n\
Packages = {{ \n\
\"VO_ALICE@AliPhysics::{aliphysics}\", \n\
\"VO_ALICE@Python-modules::1.0-12\" \n\
}}; \n\
# JDL variables \n\
JDLVariables = \n\
{{ \n\
\"Packages\", \n\
\"OutputDir\" \n\
}}; \n\
InputDataCollection={{\"LF:{dest}/{xml},nodownload\"}}; \n\
InputDataListFormat = \"xml-single\"; \n\
InputDataList = \"wn.xml\"; \n\
SplitMaxInputFileNumber=\"{maxFiles}\"; \n\
ValidationCommand = \"{dest}/{validationScript}\"; \n\
# List of input files to be uploaded to workers \n\
".format(comments=comments, executable=Exe, xml=Xml, dest=AlienDest, trainName=TrainName, aliphysics=AliPhysicsVersion, validationScript=ValidationScript, maxFiles=MaxFilesPerJob, TTL=TTL)
    if SplitMethod:
        jdlContent += "Split=\"{split}\"; \n".format(split=SplitMethod)
    if len(FilesToCopy) > 0:
        jdlContent += "InputFile = {"
        start = True
        for file in FilesToCopy:
            if start:
                jdlContent += "\n"
            else:
                jdlContent += ", \n"
            start = False
            jdlContent += "\"LF:{dest}/{f}\"".format(dest=AlienDest, f=file)
        jdlContent += "}; \n"

    return jdlContent

def DetermineMergingStage(AlienPath, TrainName):
    AlienOutput = "{0}/{1}".format(AlienPath, TrainName)
    if not aligridtools.AlienFileExists(AlienOutput):
        return -1
    AlienOuputContent_orig = alisimtools.subprocess_checkoutput(["alien_ls", AlienOutput]).splitlines()
    AlienOuputContent = []
    for p in AlienOuputContent_orig:
        i = p.rfind("/")
        if i >= 0: p = p[i + 1:]
        AlienOuputContent.append(p)
    if not "output" in AlienOuputContent:
        logging.info("%s", AlienOuputContent)
        return -1
    regex = re.compile("stage_.")
    MergingStages = [string for string in AlienOuputContent if re.match(regex, string)]
    MergingStage = len(MergingStages)
    return MergingStage

def SubmitMergingJobs(TrainName, LocalPath, AlienPath, AliPhysicsVersion, Offline, GridUpdate, TTL, MaxFilesPerJob, Gen, Proc, PtHardList, MergingStage):
    if PtHardList and len(PtHardList) > 1:
        minPtHardBin = 0
        maxPtHardBin = len(PtHardList) - 1
    else:
        minPtHardBin = -1
        maxPtHardBin = 0

    for ptHardBin in range(minPtHardBin, maxPtHardBin):
        if ptHardBin < 0:
            TrainPtHardName = TrainName
        else:
            TrainPtHardName = "{0}/{1}".format(TrainName, ptHardBin)

        if MergingStage < 0:
            MergingStage = DetermineMergingStage(AlienPath, TrainPtHardName)

        if MergingStage < 0:
            logging.error("Could not find any results from train {0}! Aborting...".format(TrainName))
            exit(1)
        elif MergingStage == 0:
            logging.info("Merging stage determined to be 0 (i.e. first merging stage)")
            PreviousStagePath = "{0}/{1}/output".format(AlienPath, TrainPtHardName)
            SplitMethod = "parentdirectory"
        else:
            logging.info("Merging stage determined to be {0}".format(MergingStage))
            PreviousStagePath = "{0}/{1}/stage_{2}/output".format(AlienPath, TrainPtHardName, MergingStage - 1)
            SplitMethod = "parentdirectory"

        AlienDest = "{0}/{1}/stage_{2}".format(AlienPath, TrainPtHardName, MergingStage)
        LocalDest = "{0}/{1}/stage_{2}".format(LocalPath, TrainPtHardName, MergingStage)

        if AlienFileExists(AlienDest): AlienDeleteDir(AlienDest)

        ValidationScript = "FastSim_validation.sh"
        ExeFile = "runFastSimMerging.py"
        JdlFile = "FastSim_Merging_{0}_{1}.jdl".format(Gen, Proc)
        XmlFile = "FastSim_Merging_{0}_{1}_stage_{2}.xml".format(Gen, Proc, MergingStage)

        FilesToCopy = ["runJetSimulationMergingGrid.C", "start_merging.C"]
        JdlContent = GenerateMergingJDL(ExeFile, XmlFile, AlienDest, TrainName, AliPhysicsVersion, ValidationScript, FilesToCopy, TTL, MaxFilesPerJob, SplitMethod)

        f = open(JdlFile, 'w')
        f.write(JdlContent)
        f.close()

        XmlContent = GenerateXMLCollection(PreviousStagePath, XmlFile)
        f = open(XmlFile, 'w')
        f.write(XmlContent)
        f.close()

        FilesToCopy.extend([JdlFile, XmlFile, ExeFile, ValidationScript])

        CopyFilesToTheGrid(FilesToCopy, AlienDest, LocalDest, Offline, GridUpdate)
        if not Offline:
            alisimtools.subprocess_call(["alien_submit", "alien://{0}/{1}".format(AlienDest, JdlFile)])
        os.remove(JdlFile)
        os.remove(XmlFile)
    logging.info("Done.")

    alisimtools.subprocess_call(["ls", LocalDest])

def SubmitProcessingJobs(TrainName, LocalPath, AlienPath, AliPhysicsVersion, Offline, GridUpdate, TTL, Events, Jobs, Gen, Proc, yamlFileName, PtHardList, OldPowhegInit, PowhegStage, HerwigTune, LoadPackagesSeparately):
    logging.info("Submitting processing jobs for train {0}".format(TrainName))

    ValidationScript = "FastSim_validation.sh"
    ExeFile = "runFastSim.py"
    JdlFile = "FastSim_{0}_{1}.jdl".format(Gen, Proc)

    FilesToDelete = [JdlFile]

    FilesToCopy = [yamlFileName, "OnTheFlySimulationGenerator.cxx", "OnTheFlySimulationGenerator.h",
                   "runJetSimulation.C", "start_simulation.C",
                   "lhapdf_utils.py",
                   "Makefile", "HepMC.tar",
                   "AliGenExtFile_dev.h", "AliGenExtFile_dev.cxx",
                   "AliGenReaderHepMC_dev.h", "AliGenReaderHepMC_dev.cxx",
                   "AliGenEvtGen_dev.h", "AliGenEvtGen_dev.cxx",
                   "AliGenPythia_dev.h", "AliGenPythia_dev.cxx",
                   "AliPythia6_dev.h", "AliPythia6_dev.cxx",
                   "AliPythia8_dev.h", "AliPythia8_dev.cxx",
                   "AliPythiaBase_dev.h", "AliPythiaBase_dev.cxx",
                   "THepMCParser_dev.h", "THepMCParser_dev.cxx"]
    
    Packages = "\"VO_ALICE@Python-modules::1.0-27\",\n"
    if not LoadPackagesSeparately:
        Packages += "\"VO_ALICE@AliPhysics::{aliphysics}\",\n".format(aliphysics=AliPhysicsVersion)

    if "pythia8" in Gen:
        FilesToCopy.append("powheg_pythia8_conf.cmnd")

    if "powheg" in Gen:
        if OldPowhegInit:
            if PowhegStage == 0:
                alipowhegtools.main(yamlFileName, "./", Events, 0)
                FilesToCopy.extend(["data/{}/pwggrid.dat".format(OldPowhegInit), "data/{}/pwgubound.dat".format(OldPowhegInit)])
            elif PowhegStage == 4:
                alipowhegtools.main(yamlFileName, "./", Events, 4)
                os.rename(alipowhegtools.GetParallelInputFileName(4), "powheg.input")
                EssentialFilesToCopy = ["pwggrid-????.dat", "pwggridinfo-btl-xg?-????.dat", "pwgubound-????.dat"]

                for fpattern in EssentialFilesToCopy:
                    for file in glob.glob("data/{}/{}".format(OldPowhegInit, fpattern)): FilesToCopy.append(file)

                seed_file_name = "pwgseeds.dat"
                FilesToDelete.append(seed_file_name)
                FilesToCopy.append(seed_file_name)
                with open(seed_file_name, "w") as seed_file:
                    for iseed in range(0, Jobs + 1):
                        seed_file.write(str(random.randint(0, 1073741824)))
                        seed_file.write("\n")
            else:
                logging.error("Not implemented for POWHEG stage {}".format(PowhegStage))
                exit(1)
        else:
            if PowhegStage != 0:
                logging.error("Not implemented for POWHEG stage {}".format(PowhegStage))
                exit(1)
            else:
                alipowhegtools.main(yamlFileName, "./", Events, 0)
        FilesToCopy.append("powheg.input")
        FilesToDelete.append("powheg.input")
        if not LoadPackagesSeparately:
            Packages += "\"VO_ALICE@POWHEG::r3178-alice1-1\",\n"
    if "herwig" in Gen:
        aliherwigtools.main(yamlFileName, "./", Events)
        FilesToCopy.extend(["herwig.in", "MB.in", "PPCollider.in", "SoftModel.in", "SoftTune.in"])
        if HerwigTune:
            FilesToCopy.append(HerwigTune)
        FilesToDelete.append("herwig.in")
        if not LoadPackagesSeparately:
            Packages += "\"VO_ALICE@Herwig::v7.1.2-alice1-3\",\n"

    if PtHardList and len(PtHardList) > 1:
        minPtHardBin = 0
        maxPtHardBin = len(PtHardList) - 1
    else:
        minPtHardBin = -1
        maxPtHardBin = 0

    Packages = Packages[:-2] # remove trailing ",\n"
    for ptHardBin in range(minPtHardBin, maxPtHardBin):
        if ptHardBin < 0:
            AlienDest = "{0}/{1}".format(AlienPath, TrainName)
            LocalDest = "{0}/{1}".format(LocalPath, TrainName)
            minPtHard = -1
            maxPtHard = -1
            JobsPtHard = Jobs
        else:
            minPtHard = PtHardList[ptHardBin]
            maxPtHard = PtHardList[ptHardBin + 1]
            AlienDest = "{0}/{1}/{2}".format(AlienPath, TrainName, ptHardBin)
            LocalDest = "{0}/{1}/{2}".format(LocalPath, TrainName, ptHardBin)
            JobsPtHard = Jobs[ptHardBin]
        JdlContent = GenerateProcessingJDL(ExeFile, AlienDest, Packages, ValidationScript, FilesToCopy, TTL, Events, JobsPtHard, yamlFileName, minPtHard, maxPtHard, PowhegStage)

        f = open(JdlFile, 'w')
        f.write(JdlContent)
        f.close()

        FilesToCopy.extend([JdlFile, ExeFile, ValidationScript])

        aligridtools.CopyFilesToTheGrid(FilesToCopy, AlienDest, LocalDest, Offline, GridUpdate)
        if not Offline:
            alisimtools.subprocess_call(["alien_submit", "alien://{0}/{1}".format(AlienDest, JdlFile)])
        for file in FilesToDelete: os.remove(file)
    logging.info("Done.")

    alisimtools.subprocess_call(["ls", LocalDest])

def DownloadResults(TrainName, LocalPath, AlienPath, Gen, Proc, PtHardList, MergingStage):
    if PtHardList and len(PtHardList) > 1:
        minPtHardBin = 0
        maxPtHardBin = len(PtHardList) - 1
    else:
        minPtHardBin = -1
        maxPtHardBin = 0

    for ptHardBin in range(minPtHardBin, maxPtHardBin):
        if ptHardBin < 0:
            TrainPtHardName = TrainName
        else:
            minPtHard = PtHardList[ptHardBin]
            maxPtHard = PtHardList[ptHardBin + 1]
            TrainPtHardName = "{0}/{1}".format(TrainName, ptHardBin)
        logging.info("Downloading results from train {0}".format(TrainPtHardName))
        if MergingStage < 0:
            MergingStage = DetermineMergingStage(AlienPath, TrainPtHardName)

        if MergingStage < 0:
            logging.error("Could not find any results from train {0}! Aborting...".format(TrainPtHardName))
            exit(0)
        elif MergingStage == 0:
            logging.warning("Merging stage determined to be 0 (i.e. no grid merging has been performed)")
            AlienOutputPath = "{0}/{1}/output".format(AlienPath, TrainPtHardName)
            LocalDest = "{0}/{1}/output".format(LocalPath, TrainPtHardName)
        else:
            logging.info("Merging stage determined to be {0}".format(MergingStage))
            AlienOutputPath = "{0}/{1}/stage_{2}/output".format(AlienPath, TrainPtHardName, MergingStage - 1)
            LocalDest = "{0}/{1}/stage_{2}/output".format(LocalPath, TrainPtHardName, MergingStage - 1)

        if not os.path.isdir(LocalDest):
            os.makedirs(LocalDest)
        AlienOuputContent = alisimtools.subprocess_checkoutput(["alien_ls", AlienOutputPath]).splitlines()
        for SubDir in AlienOuputContent:
            i = SubDir.rfind("/")
            if i >= 0: SubDir = SubDir[i + 1:]
            SubDirDest = "{0}/{1}".format(LocalDest, SubDir)
            SubDirOrig = "{0}/{1}".format(AlienOutputPath, SubDir)
            if not os.path.isdir(SubDirDest):
                os.makedirs(SubDirDest)
            FilesToDownload = alisimtools.subprocess_checkoutput(["alien_ls", "{0}/AnalysisResults*.root".format(SubDirOrig)]).splitlines()
            for FileName in FilesToDownload:
                i = FileName.rfind("/")
                if i >= 0: FileName = FileName[i + 1:]
                FileDest = "{0}/{1}".format(SubDirDest, FileName)
                if os.path.isfile(FileDest):
                    logging.warning("File {0} already exists, skipping...".format(FileDest))
                    continue
                FileOrig = "{0}/{1}".format(SubDirOrig, FileName)
                FileDestTemp = "{0}/temp_{1}".format(SubDirDest, FileName)
                if os.path.isfile(FileDestTemp):
                    os.remove(FileDestTemp)
                logging.info("Downloading from {0} to {1}".format(FileOrig, FileDestTemp))
                alisimtools.subprocess_call(["alien_cp", "alien://{0}".format(FileOrig), FileDestTemp])
                if os.path.getsize(FileDestTemp) > 0:
                    logging.info("Renaming {0} to {1}".format(FileDestTemp, FileDest))
                    os.rename(FileDestTemp, FileDest)
                else:
                    logging.error("Downloading of {0} failed!".format(FileOrig))
                    os.remove(FileDestTemp)

def GetLastTrainName(AlienPath, Gen, Proc):
    TrainName = "FastSim_{0}_{1}".format(Gen, Proc)
    AlienPathContent = alisimtools.subprocess_checkoutput(["alien_ls", AlienPath]).splitlines()
    regex = re.compile("{0}.*".format(TrainName))
    Timestamps = [int(subdir[len(TrainName) + 1:]) for subdir in AlienPathContent if re.match(regex, subdir)]
    if len(Timestamps) == 0:
        logging.error("Could not find any train in the alien path {0} provided!".format(AlienPath))
        logging.error("\n".join(AlienPathContent))
        logging.error("{0}.*".format(TrainName))
        return None
    TrainName += "_{0}".format(max(Timestamps))
    return TrainName

def GetAliPhysicsVersion(ver):
    if ver == "_last_":
        now = datetime.datetime.now()
        if now.hour < 18: now -= datetime.timedelta(days=1)
        ver = now.strftime("vAN-%Y%m%d-1")
    return ver

def main(UserConf, yamlFileName, Offline, GridUpdate, OldPowhegInit, PowhegStage, Merge, Download, MergingStage):
    f = open(yamlFileName, 'r')
    config = yaml.load(f, yaml.SafeLoader)
    f.close()

    if "load_packages_separately" in config["grid_config"]:
        LoadPackagesSeparately = config["grid_config"]["load_packages_separately"]
    else:
        LoadPackagesSeparately = False
    AliPhysicsVersion = GetAliPhysicsVersion(config["grid_config"]["aliphysics"])
    Events = config["numevents"]
    Jobs = config["numbjobs"]
    Gen = config["gen"]
    Proc = config["proc"]
    if "pthard" in config:
        PtHardList = config["pthard"]
    else:
        PtHardList = False
    TTL = config["grid_config"]["ttl"]
    MaxFilesPerJob = config["grid_config"]["max_files_per_job"]

    if "herwig_config" in config and "tune" in config["herwig_config"]:
        HerwigTune = config["herwig_config"]["tune"]
    else:
        HerwigTune = None

    try:
        rootPath = subprocess.check_output(["which", "root"]).decode(sys.stdout.encoding).rstrip()
        alirootPath = subprocess.check_output(["which", "aliroot"]).decode(sys.stdout.encoding).rstrip()
        alienPath = subprocess.check_output(["which", "alien-token-info"]).decode(sys.stdout.encoding).rstrip()
    except subprocess.CalledProcessError:
        logging.error("Environment is not configured correctly!")
        exit()

    logging.info("Root:    %s", rootPath)
    logging.info("AliRoot: %s", alirootPath)
    logging.info("Alien:   %s", alienPath)

    try:
        logging.info("Token info disabled")
        # tokenInfo=subprocess.check_output(["alien-token-info"])
    except subprocess.CalledProcessError:
        logging.info("Alien token not available. Creating a token for you...")
        try:
            # tokenInit=subprocess.check_output(["alien-token-init", UserConf["username"]], shell=True)
            logging.info("Token init disabled")
        except subprocess.CalledProcessError:
            logging.error("Error: could not create the token!")
            exit()

    LocalPath = UserConf["local_path"]
    AlienPath = "/alice/cern.ch/user/{0}/{1}".format(UserConf["username"][0], UserConf["username"])

    logging.info("Local working directory: {0}".format(LocalPath))
    logging.info("Alien working directory: {0}".format(AlienPath))

    if Merge:
        if Merge == "last":
            TrainName = GetLastTrainName(AlienPath, Gen, Proc)
            if not TrainName:
                exit(1)
        else:
            TrainName = "FastSim_{0}_{1}_{2}".format(Gen, Proc, Merge)
        SubmitMergingJobs(TrainName, LocalPath, AlienPath, AliPhysicsVersion, Offline, GridUpdate, TTL, MaxFilesPerJob, Gen, Proc, PtHardList, MergingStage)
    elif Download:
        if Download == "last":
            TrainName = GetLastTrainName(AlienPath, Gen, Proc)
            if not TrainName:
                exit(1)
        else:
            TrainName = "FastSim_{0}_{1}_{2}".format(Gen, Proc, Download)
        DownloadResults(TrainName, LocalPath, AlienPath, Gen, Proc, PtHardList, MergingStage)
    else:
        unixTS = int(time.time())
        logging.info("The timestamp for this job is %d. You will need it to submit merging jobs and download you final results.", unixTS)
        TrainName = "FastSim_{0}_{1}_{2}".format(Gen, Proc, unixTS)
        SubmitProcessingJobs(TrainName, LocalPath, AlienPath, AliPhysicsVersion, Offline, GridUpdate, TTL, Events, Jobs, Gen, Proc, yamlFileName, PtHardList, OldPowhegInit, PowhegStage, HerwigTune, LoadPackagesSeparately)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Local final merging for LEGO train results.')
    parser.add_argument('config', metavar='config.yaml',
                        default="default.yaml", help='YAML configuration file')
    parser.add_argument('--user-conf', metavar='USERCONF',
                        default="userConf.yaml")
    parser.add_argument("--update", action='store_const',
                        default=False, const=True,
                        help='Update all scripts and macros on the grid.')
    parser.add_argument('--offline', action='store_const',
                        default=False, const=True,
                        help='Test mode')
    parser.add_argument('--merge', metavar='TIMESTAMP',
                        default='')
    parser.add_argument('--download', metavar='TIMESTAMP',
                        default='')
    parser.add_argument('--stage', metavar='stage',
                        default=-1, type=int)
    parser.add_argument('--old-powheg-init', metavar='folder',
                        default=None)
    parser.add_argument("--powheg-stage",
                        default=0, type=int)
    parser.add_argument('-d', '--debug', action = "store_true",  help = "Run with increased debug level")
    args = parser.parse_args()

    loglevel=logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    logging.basicConfig(format='[%(levelname)s]: %(message)s', level=loglevel)

    userConf = aliuserconfig.LoadUserConfiguration(args.user_conf)

    main(userConf, args.config, args.offline, args.update, args.old_powheg_init, args.powheg_stage, args.merge, args.download, args.stage)
