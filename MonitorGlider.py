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
from ParseDialog import ParseDialog

class MonitorGlider(Thread):
    def __init__(self, glider:str, args:ArgumentParser, parser:ParseDialog) -> None:
        Thread.__init__(self, glider, args)
        self.__gliderName = glider
        self.__parser = parser

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        try:
            grp = parser.add_argument_group(description="SFMC API")
            grp.add_argument("--API", type=str, default="./sfmc-rest-programs",
                    help="Where SFMC API's Javascripts are")
            grp.add_argument("--node", type=str, default="/usr/bin/node",
                    help="Which node command to execute")
            grp.add_argument("--reconnect", type=int, default=10,
                    help="Number of reconnection attempts to allow")
        except:
            pass
        return parser


    def runIt(self): # Called on start
        args = self.args
        cmd = (args.node,
                os.path.join(args.API, "output_glider_dialog_data.js"),
                self.__gliderName,
                )

        logging.info("Starting %s", cmd)

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
                logging.info("Line %s", line)
                if not line: break
                self.__parser.put(line)
        raise Exception(f"To many reconnection attempts, {cnt}")
