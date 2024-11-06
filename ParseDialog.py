#! /usr/bin/env python3
#
# Extract the latitude/longitude from the SFMC dialog
#
# This is a rewrite of my existing code for handling SFMC's API
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
import queue
import os
import re
import math
from datetime import datetime, timezone, timedelta
from TPWUtils.Thread import Thread

class ParseDialog(Thread):
    def __init__(self, glider:str, args:ArgumentParser, sendTo:list) -> None:
        Thread.__init__(self, glider, args)
        self.__gliderName = glider
        self.__sendTo = sendTo
        self.__queue = queue.Queue()
        self.__location = re.compile(
                b"^GPS Location:\s+([+-]?\d+[.]\d+)\s+[NS]\s+([+-]?\d+[.]\d+)\s+[EW]\s+measured\s+(\d+[.]\d+)\s+secs")
        self.__time = re.compile(
                b"^Curr Time: \w+ (\w+\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})")

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Dialog Parser")
        grp.add_argument("--csvDir", type=str, default="./CSV")

    def put(self, line:bytes) -> None:
        self.__queue.put(line)

    @staticmethod
    def __mkDegrees(degmin:bytes) -> float:
        degmin = float(str(degmin, "utf-8"))
        qNeg = -1 if degmin < 0 else +1
        degmin = abs(degmin)
        deg = math.floor(degmin / 100)
        minutes = degmin % 100
        return qNeg * (deg + minutes / 60)

    def runIt(self): # Called on start
        ofn = os.path.join(self.args.csvDir, self.__gliderName + ".csv")
        logging.info("Starting %s", ofn)

        if not os.path.isdir(self.args.csvDir):
            logging.info("Creating %s", self.args.csvDir)
            os.makedirs(self.args.csvDir, mode=0o755, exist_ok=True)

        q = self.__queue
        sendTo = self.__sendTo

        reLoci = self.__location
        reTime = self.__time

        time = None
        prevTime = None

        while True:
            line = q.get()
            matches = reLoci.match(line)
            if matches:
                lat = self.__mkDegrees(matches[1])
                lon = self.__mkDegrees(matches[2])
                dt = float(str(matches[3], "utf-8"))
                if (abs(lat) > 90) or (abs(lon) > 180) or (abs(dt) > 1e10): continue
                if not time: continue
                t = time - timedelta(seconds=dt)
                if prevTime is None or (abs(t - prevTime) > timedelta(seconds=60)):
                    prevTime = t
                    with open(ofn, "a") as fp:
                        if fp.tell() == 0:
                            fp.write("time,lat,lon\n")
                        fp.write(f"{time.timestamp():.0f},{lat:.7f},{lon:.7f}\n")
                time = None
                if sendTo: 
                    for tgt in sendTo: 
                        tgt.put(ofn)
                continue
            matches = reTime.match(line)
            if matches:
                time = datetime.strptime(
                        str(matches[1], "utf-8"),
                        "%b %d %H:%M:%S %Y",
                        ).replace(tzinfo=timezone.utc)
                continue

if __name__ == "__main__":
    from TPWUtils import Logger
    from SendTo import SendToTarget

    parser = ArgumentParser()
    parser.add_argument("dialog", type=str, nargs="+", help="Dialog log file(s) to parse")
    Logger.addArgs(parser)
    SendToTarget.addArgs(parser)
    ParseDialog.addArgs(parser)
    args = parser.parse_args()

    Logger.mkLogger(args, logLevel=logging.INFO)

    logging.info("args %s", args)
    sendTo = []
    if args.hostname:
        for tgt in args.hostname:
            sendTo.append(SendToTarget(tgt, args))
            sendTo[-1].start()

    thrd = ParseDialog("probar", args, sendTo)
    thrd.start()

    for fn in args.dialog:
        with open(fn, "rb") as fp:
            while True:
                line = fp.readline()
                if not line: break
                thrd.put(line)
    try:
        Thread.waitForException(timeout=20)
    except:
        logging.exception("Unexpected termination")
