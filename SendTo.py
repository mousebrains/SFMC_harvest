#! /usr/bin/env python3
#
# Sync a file up to a target machine via rsync
#
# This is a rewrite of my existing code for handling SFMC's API
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import subprocess
import logging
import queue
from TPWUtils import Logger
from TPWUtils.Thread import Thread

class SendToTarget(Thread):
    def __init__(self, tgt:str, args:ArgumentParser) -> None:
        Thread.__init__(self, tgt, args)
        self.__queue = queue.Queue() #  For receiving messages

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Send To")
        grp.add_argument("--hostname", type=str, action="append",
                help="Where to send CSV files to using rsync")
        grp.add_argument("--tempDirectory", type=str, default="~/.cache",
                help="Where to write temporary files on the target")
        grp.add_argument("--rsync", type=str, default="/usr/bin/rsync", help="rsync command to use")
        return parser

    def put(self, fn:str) -> None:
        self.__queue.put(fn)

    def runIt(self): # Called on start
        logging.info("Starting")
        q = self.__queue

        while True:
            fn = q.get()
            logging.info("SendTo %s", fn)
            cmd = (
                    self.args.rsync,
                    "--archive",
                    "--verbose",
                    "--temp-dir", self.args.tempDirectory,
                    "--delay-updates",
                    fn,
                    self.name)
            logging.info("cmd %s", cmd)
            sp = subprocess.run(cmd, shell=False, capture_output=True)
            if sp.returncode:
                logging.warning("cmd %s", cmd)
                logging.warning("return code %s", sp.returncode)
                if sp.stdout:
                    try:
                        logging.warning("STDOUT: %s", str(sp.stdout, "utf-8"))
                    except:
                        logging.warning("STDOUT: %s", sp.stdout)
                if sp.stderr:
                    try:
                        logging.warning("STDERR: %s", str(sp.stderr, "utf-8"))
                    except:
                        logging.warning("STDERR: %s", sp.stderr)
            else:
                logging.info("cmd %s", cmd)
                if sp.stdout:
                    try:
                        logging.info("STDOUT: %s", str(sp.stdout, "utf-8"))
                    except:
                        logging.info("STDOUT: %s", sp.stdout)
                if sp.stderr:
                    try:
                        logging.info("STDERR: %s", str(sp.stderr, "utf-8"))
                    except:
                        logging.info("STDERR: %s", sp.stderr)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("file", type=str, nargs="+", help="Name of files to send")
    Logger.addArgs(parser)
    SendToTarget.addArgs(parser)
    args = parser.parse_args()

    Logger.mkLogger(args, logLevel=logging.INFO)

    sendTo = []
    if args.hostname:
        for tgt in args.hostname:
            sendTo.append(SendToTarget(tgt, args))
            sendTo[-1].start()

    for fn in args.file:
        for tgt in sendTo:
            tgt.put(fn)

    try:
        Thread.waitForException(timeout=10)
    except:
        logging.exception("Unexpected termination")
