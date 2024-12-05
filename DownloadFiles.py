#! /usr/bin/env python3
#
# When triggered, download all files that match a specified pattern, then unzip them and put them into a target directory
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
import queue
import time
import zipfile
import json
from tempfile import TemporaryDirectory
from datetime import datetime, timezone, timedelta
from TPWUtils.Thread import Thread
from SendTo import SendToTarget

class DownloadFiles(Thread):
    def __init__(self, glider:str, args:ArgumentParser, sendTo:SendToTarget) -> None:
        Thread.__init__(self, "DN:" + glider, args)
        self.__glider = glider
        self.__sendTo = sendTo
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Download files options")
        grp.add_argument("--folder", type=str, default="from-glider",
                help="Folder to pull files from")
        grp.add_argument("--safety", type=int, default=86400,
                help="Seconds back to look for files after the first fetch")
        grp.add_argument("--downloadDelay", type=float, default=300,
                help="How long to delay in seconds")
        return parser

    def join(self) -> None:
        self.__queue.join()

    def put(self, time:float) -> None:
        self.__queue.put(time)

    def runIt(self): # Called on start
        q = self.__queue
        args = self.args
        glider = self.__glider
        safety = timedelta(seconds=args.safety)

        logging.info("Starting safety %s", safety)

        while True:
            t0 = q.get()
            logging.info("Sleeping for %s seconds", args.downloadDelay)
            time.sleep(args.downloadDelay)
            while not q.empty(): # Eat anything pending
                q.get()
                q.task_done()

            with TemporaryDirectory() as tgtDir:
                fnZip = os.path.join(tgtDir, "__temp__.zip")
                cmd = (args.node,
                        os.path.join(args.API, "download_glider_files.js"),
                        glider,
                        args.folder,
                        "*.*",
                        (t0 - safety).strftime("%Y%m%d%H%M"),
                        fnZip,
                        )
                logging.info("cmd %s", cmd)
                sp = subprocess.run(cmd, 
                        shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if sp.returncode != 0:
                    logging.error("%s", sp)
                if not os.path.isfile(fnZip):
                    q.task_done()
                    continue
                logging.info("Unzipping %s", fnZip)
                tgtPath = os.path.join(tgtDir, glider)
                os.makedirs(tgtPath, 0o755, exist_ok=True)
                with zipfile.ZipFile(fnZip) as zip:
                    cnt = len(zip.namelist())
                    zip.extractall(path=tgtPath)
                os.unlink(fnZip)
                logging.info("Retrieved %s files", cnt)
                if self.__sendTo:
                    for tgt in self.__sendTo: tgt.put(tgtPath)
                    for tgt in self.__sendTo: tgt.join()

            q.task_done()

if __name__ == "__main__":
    from TPWUtils import Logger

    parser = ArgumentParser()
    Logger.addArgs(parser)
    DownloadFiles.addArgs(parser)
    parser.add_argument("glider", type=str, help="Glider to download files for")
    parser.add_argument("--API", type=str, default="./sfmc-rest-programs",
            help="Where SFMC API's Javascripts are")
    parser.add_argument("--node", type=str, default="/usr/bin/node",
            help="Which node command to execute")
    args = parser.parse_args()

    Logger.mkLogger(args)

    dn = DownloadFiles(args.glider, args, None)
    dn.start()
    dn.put(datetime.now(tz=timezone.utc))
    try:
        Thread.waitForException(20)
    except:
        logging.exception("Unexpected")
