#! /usr/bin/env python

import shutil
from alifastsim import Tools as alisimtools

def AlienDelete(fileName):
    if fileName.find("alien://") == -1:
        fname = fileName
    else:
        fname = fileName[8:]
    alisimtools.subprocess_call(["alien_rm", fname])

def AlienDeleteDir(fileName):
    if fileName.find("alien://") == -1:
        fname = fileName
    else:
        fname = fileName[8:]
    alisimtools.subprocess_call(["alien_rmdir", fname])

def AlienFileExists(fileName):
    if fileName.find("alien://") == -1:
        fname = fileName
    else:
        fname = fileName[8:]

    fileExists = True
    try:
        alisimtools.subprocess_checkcall(["alien_ls", fname])
    except subprocess.CalledProcessError:
        fileExists = False

    return fileExists

def AlienCopy(source, destination, attempts=3, overwrite=False):
    i = 0
    fileExists = False

    if AlienFileExists(destination):
        if overwrite:
            AlienDelete(destination)
        else:
            return True

    if destination.find("alien://") == -1:
        dest = "alien://{0}".format(destination)
    else:
        dest = destination

    while True:
        alisimtools.subprocess_call(["alien_cp", source, dest])
        i += 1
        fileExists = AlienFileExists(destination)
        if fileExists:
            break
        if i >= attempts:
            print("After {0} attempts I could not copy {1} to {2}".format(i, source, dest))
            break

    return fileExists

def CopyFilesToTheGrid(Files, AlienDest, LocalDest, Offline, GridUpdate):
    if not Offline:
        alisimtools.subprocess_call(["alien_mkdir", "-p", AlienDest])
        alisimtools.subprocess_call(["alien_mkdir", "-p", "{0}/output".format(AlienDest)])

    if not os.path.isdir(LocalDest):
        print "Creating directory " + LocalDest
        os.makedirs(LocalDest)
    for file in Files:
        fname = os.path.basename(file)
        if not Offline:
            AlienCopy(file, "alien://{}/{}".format(AlienDest, fname), 3, GridUpdate)
        shutil.copy(file, os.path.join(LocalDest, fname))