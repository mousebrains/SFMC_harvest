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
import random
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
        random.seed(time.time())

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Download files options")
        grp.add_argument("--folder", type=str, default="from-glider",
                help="Folder to pull files from")
        grp.add_argument("--safety", type=int, default=86400/2,
                help="Seconds back to look for files after the first fetch")
        grp.add_argument("--downloadDelay", type=float, default=300,
                help="How long to delay in seconds")
        grp.add_argument("--randomDelay", type=float, default=60,
                help="Random delay between page fetches in seconds")
        return parser

    def join(self) -> None:
        self.__queue.join()

    def put(self) -> None:
        self.__queue.put(None)

    def __fileTimes(self, t0:datetime, files:dict) -> tuple:
        # newest files are in page 0 and oldest in last page
        args = self.args
        files = dict()
        tMin = None
        tMax = None
        page = 0

        while True: 
            cmd = (
                    args.node,
                    os.path.join(args.API, "get_glider_folder_listing.js"),
                    self.__glider,
                    args.folder,
                    str(page),
                    )
            sp = subprocess.run(cmd, 
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    )
            if sp.returncode != 0:
                logging.error("Executing %s", cmd)
                if sp.stderr: logging.error("STDERR: %s", sp.stderr)
                if sp.stdout: logging.error("STDOUT: %s", sp.stdout)
                break

            try:
                info = json.loads(sp.stdout)
            except:
                logging.exception("Unable to parse %s", sp.stdout)
                continue

            for item in info["results"]:
                mtime = datetime.strptime(item["dateTimeModified"], "%Y-%m-%d %H:%M:%S") \
                        .replace(tzinfo=timezone.utc)
                tMin = mtime if tMin is None else min(mtime, tMin)
                tMax = mtime if tMax is None else max(mtime, tMax)
                files[item["fileName"]] = mtime

            if t0 is not None and tMin is not None and tMin <= t0: break
            if "next" not in info["links"]: break

            page += 1
            dt = random.uniform(0.5,args.randomDelay)
            logging.info("Waiting %s seconds to throttle page requests before page %s", dt, page)
            time.sleep(dt)

        return (files, tMin, tMax)

    def __fetchFiles(self, t0:datetime, fileTimes:dict) -> set:
        args = self.args
        fetched = None
        with TemporaryDirectory() as tgtDir:
            fnZip = os.path.join(tgtDir, "__temp__.zip")
            cmd = (
                    args.node,
                    os.path.join(args.API, "download_glider_files.js"),
                    self.__glider,
                    args.folder,
                    "*.*",
                    (t0 - timedelta(seconds=args.safety)).strftime("%Y%m%d%H%M"),
                    fnZip,
                    )
            sp = subprocess.run(cmd,
                    shell=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    )
            if sp.returncode != 0:
                logging.error("Executing %s", cmd)
                if sp.stderr: logging.error("STDERR: %s", sp.stderr)
                if sp.stdout: logging.error("STDOUT: %s", sp.stdout)
                return None

            if not os.path.isfile(fnZip):
                logging.warning("%s was not created!", fnZip)
                if sp.stderr: logging.warning("STDERR: %s", sp.stderr)
                if sp.stdout: logging.warning("STDOUT: %s", sp.stdout)
                return None

            tgtPath = os.path.join(tgtDir, self.__glider)
            os.makedirs(tgtPath, 0o755, exist_ok=True)
            with zipfile.ZipFile(fnZip) as zip:
                cnt = len(zip.namelist())
                logging.info("Downloaded %s files in %s", cnt, fnZip)
                zip.extractall(path=tgtPath)
            os.unlink(fnZip)
            fetched = set()
            for fn in os.listdir(tgtPath):
                mtime = fileTimes[fn] if fn in fileTimes else None
                fetched.add(fn)
                if not mtime: continue
                mtime = mtime.timestamp()
                os.utime(os.path.join(tgtPath, fn), times=(mtime,mtime))

            if self.__sendTo:
                for tgt in self.__sendTo: tgt.put(tgtPath) # Rsync everything over
                # Wait for the rsyncs to finish before we delete directory
                for tgt in self.__sendTo: 
                    logging.info("Joining %s", tgt)
                    tgt.join()

        return fetched

    def runIt(self): # Called on start
        q = self.__queue
        args = self.args
        glider = self.__glider
        safety = timedelta(seconds=args.safety)

        logging.info("Starting safety %s", safety)

        [fileTimes, t0, t1] = self.__fileTimes(None, dict()) # Get the initial list of files
        logging.info("t0 %s t1 %s n %s", t0, t1, len(fileTimes))
        # Fetch all the files with times >= t0-safety
        if t0 is not None: 
            self.__fetchFiles(t0, fileTimes) 

        while True:
            logging.info("Waiting on queue")
            q.get()
            logging.info("Sleeping for %s seconds", args.downloadDelay)
            time.sleep(args.downloadDelay)
            while not q.empty(): # Eat anything pending
                q.get()
                q.task_done()

            [fileTimes, t0, t1] = self.__fileTimes(t1, fileTimes)
            logging.info("n %s t0 %s t1 %s", len(fileTimes), t0, t1)
            if t0 is not None:
                self.__fetchFiles(t0, fileTimes)
                logging.info("Returned from __fetchFiles")
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
    logging.info("Putting")
    dn.put()
    time.sleep(15)
    logging.info("Putting")
    dn.put()
    try:
        Thread.waitForException(40)
    except:
        logging.exception("Unexpected")
