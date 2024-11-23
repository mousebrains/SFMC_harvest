#! /usr/bin/env python3
#
# Monitor glider(s) console dialog and harvest postion and other information
#
# Optionally send the information to another computer
#
# This is a rewrite of my existing code for handling SFMC's API
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import subprocess
import logging
import os
from TPWUtils import Logger
from TPWUtils.Thread import Thread
from SendTo import SendToTarget
from ParseDialog import ParseDialog
from Sensors import Sensors
from DownloadFiles import DownloadFiles
from MonitorGlider import MonitorGlider

parser = ArgumentParser()
parser.add_argument("glider", type=str, nargs="+", help="Name of glider(s) to monitor")
Logger.addArgs(parser)
SendToTarget.addArgs(parser)
ParseDialog.addArgs(parser)
Sensors.addArgs(parser)
DownloadFiles.addArgs(parser)
MonitorGlider.addArgs(parser)
args = parser.parse_args()

Logger.mkLogger(args, logLevel=logging.INFO)

sendTo = []
if args.hostname:
    for tgt in args.hostname:
        sendTo.append(SendToTarget(tgt, args))
        sendTo[-1].start()

threads = []
for glider in args.glider:
    sensors = Sensors(glider, args, sendTo)
    download = DownloadFiles(glider, args, sendTo)
    parser = ParseDialog(glider, args, sendTo, sensors, download)
    sensors.start()
    download.start()
    parser.start()
    threads.append(MonitorGlider(glider, args, parser))
    threads[-1].start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected termination")
