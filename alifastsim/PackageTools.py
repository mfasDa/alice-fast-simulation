#! /usr/bin/env python
from alifastsim import Tools as alisimtools

def GenerateComments():
    branch = alisimtools.subprocess_checkoutput(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    hash = alisimtools.subprocess_checkoutput(["git", "rev-parse", "HEAD"])
    comments = "# This is the startup script \n\
# alice-yale-hfjet \n\
# Generated using branch {branch} ({hash}) \n\
".format(branch=branch.strip('\n'), hash=hash.strip('\n'))
    return comments