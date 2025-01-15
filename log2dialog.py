#! /usr/bin/env python3
#
# Go through an output log and pull out the dialog lines for replaying
#
# Jan-2025, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import re
import os.path

parser = ArgumentParser()
parser.add_argument("logfile", type=str, help="Input logfile name")
parser.add_argument("--glider", type=str, action="append", help="Which glider(s) to filter on")
parser.add_argument("--outdir", type=str, default=".", help="Where to write output to")
args = parser.parse_args()

ofp = {}
with open(args.logfile, "rb") as ifp:
    for line in ifp:
        if b" INFO: Line b'" not in line: continue
        if b" PD:" in line: continue
        line = line.strip()
        fields = line.split()
        if len(fields) < 6: continue
        glider = str(fields[2], "utf-8")
        if args.glider and glider not in args.glider: continue
        fields = line.split(b" b'")
        if len(fields) != 2: continue
        fields = fields[1][:-5]
        if glider not in ofp:
            ofp[glider] = open(os.path.join(args.outdir, glider + ".dialog"), "wb")
        ofp[glider].write(fields + b"\r\n")

for glider in ofp:
    ofp[glider].close()
