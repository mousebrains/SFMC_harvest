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

class MonitorGlider(Thread):
    def __init__(self, glider:str, args:ArgumentParser, parser:list) -> None:
        Thread.__init__(self, glider, args)
        self.__gliderName = glider
        self.__parser = parser

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="SFMC API")
        grp.add_argument("--API", type=str, default="./sfmc-rest-programs",
                help="Where SFMC API's Javascripts are")
        grp.add_argument("--node", type=str, default="/usr/bin/node",
                help="Which node command to execute")
        grp.add_argument("--reconnect", type=int, default=10,
                help="Number of reconnection attempts to allow")
        return parser

    def runIt(self): # Called on start
        cmd = (self.args.node,
                os.path.join(self.args.API, "output_glider_dialog_data.js"),
                self.__gliderName,
                )

        for cnt in range(args.reconnect):
            logging.info("cnt %s cmd %s", cnt, cmd)
            proc = subprocess.Popen(
                    cmd,
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    )
            while True:
                line = proc.stdout.readline()
                if not line: break
                logging.info("%s", line)
        raise Exception(f"To many reconnection attempts, {cnt}")

parser = ArgumentParser()
parser.add_argument("glider", type=str, nargs="+", help="Name of glider(s) to monitor")
Logger.addArgs(parser)
SendToTarget.addArgs(parser)
ParseDialog.addArgs(parser)
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
    parser = ParseDialog(glider, args, sendTo)
    parser.start()
    threads.append(MonitorGlider(glider, args, parser))
    threads[-1].start()

try:
    Thread.waitForException()
except:
    logging.exception("Unexpected termination")
