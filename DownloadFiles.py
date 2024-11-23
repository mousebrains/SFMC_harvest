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
from tempfile import TemporaryDirectory
from datetime import datetime, timezone
from TPWUtils.Thread import Thread
from SendTo import SendToTarget

class DownloadFiles(Thread):
    def __init__(self, glider:str, args:ArgumentParser, sendTo:SendToTarget) -> None:
        Thread.__init__(self, glider + " Dn", args)
        self.__glider = glider
        self.__sendTo = sendTo
        self.__queue = queue.Queue()

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        try:
            from MonitorGlider import MonitorGlider
            MonitorGlider.addArgs(parser)
        except:
            pass
        grp = parser.add_argument_group(description="Download files options")
        grp.add_argument("--folder", type=str, default="from-glider",
                help="Folder to pull files from")
        grp.add_argument("--downloadDir", type=str, default="./from-glider",
                help="Where to store files")
        grp.add_argument("--safety", type=float, default=600,
                help="Seconds to look for file before it was finished")
        grp.add_argument("--downloadDelay", type=float, default=60,
                help="How long to delay in seconds")
        return parser

    def put(self, fn:str, t:datetime) -> None:
        self.__queue.put((fn, t))

    def runIt(self): # Called on start
        args = self.args
        glider = self.__glider
        tgtDir = os.path.join(args.downloadDir, glider)
        logging.info("Starting tgt %s", tgtDir)

        q = self.__queue

        tPrev = None

        while True:
            (fn, t) = q.get()
            q.task_done()

            fn = str(fn, "utf-8")
            (basename, ext) = os.path.splitext(os.path.basename(fn))
            prefixes = set()
            prefixes.add(basename)

            dt = max(0.1, t + args.downloadDelay - time.time())
            logging.info("Sleeping for %s seconds for %s", dt, fn)

            time.sleep(dt)
            while not q.empty():
                (fn, tt) = q.get()
                q.task_done()
                fn = str(fn, "utf-8")
                (basename, ext) = os.path.splitext(os.path.basename(fn))
                prefixes.add(basename)

            logging.info("Prefixes %s", prefixes)

            t = t - args.safety
            if tPrev:
                t = max(t, tPrev)
            tPrev = t
            t = datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y%m%d%H%M")

            with TemporaryDirectory() as tempdir:
                fnZip = os.path.join(tempdir, "temp.zip")
                for prefix in prefixes:
                    cmd = (args.node,
                            os.path.join(args.API, "download_glider_files.js"),
                            glider,
                            args.folder,
                            prefix + ".*",
                            t,
                            fnZip,
                            )
                    sp = subprocess.run(
                            cmd,
                            shell=False, 
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            )
                    if os.path.isfile(fnZip):
                        with zipfile.ZipFile(fnZip) as zip:
                            names = zip.namelist()
                            if not names: continue
                            zip.extractall(path=tgtDir)
                            if self.__sendTo:
                                for tgt in self.__sendTo:
                                    for name in names:
                                        tgt.put(os.path.join(tgtDir, name))

                        os.unlink(fnZip)
