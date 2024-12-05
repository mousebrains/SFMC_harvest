#! /usr/bin/env python3
#
# Extract the iformation from the SFMC dialog
#  - position into a CSV
#  - position+sensors into a NetCDF
#  - uploaded [st][bc]d files
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
import time
from datetime import datetime, timezone, timedelta
from TPWUtils.Thread import Thread
from DownloadFiles import DownloadFiles
from Sensors import Sensors

class ParseDialog(Thread):
    def __init__(self, glider:str, args:ArgumentParser, sendTo:list, 
            sensors:Sensors, download:DownloadFiles,
            ) -> None:
        Thread.__init__(self, "PD:" + glider, args)
        self.__gliderName = glider
        self.__sendTo = sendTo
        self.__sensors = sensors
        self.__download = download
        self.__queue = queue.Queue()
        self.__location = re.compile(
                b"^GPS Location:\s+([+-]?\d+[.]\d+)\s+[NS]\s+([+-]?\d+[.]\d+)\s+[EW]\s+measured\s+(\d+[.]\d+)\s+secs")
        self.__time = re.compile(
                b"^Curr Time: \w+ (\w+\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\d{4})")
        self.__sensor = re.compile(
                b"^\s+sensor:(\w+)[(]([/\-%\w]+)[)]=(-?\d+[.]{0,1}\d*)\s+(\d+[.]\d*|\d+e[+-]?\d+) secs ago")
        self.__devices = re.compile(b"^devices:")
        self.__zmodem = re.compile(b"^zModem\s+transfer\s+DONE\s+for\s+file\s+(\w+[.]\w+)")

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Dialog Parser")
        grp.add_argument("--csvDir", type=str, default="./CSV", 
                help="Where to write position CSV to")

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

    def __matchedLocation(self, matches, time:datetime, prevTime:datetime, ofn:str) -> datetime:
        lat = self.__mkDegrees(matches[1])
        lon = self.__mkDegrees(matches[2])
        dt = float(str(matches[3], "utf-8"))
        if (abs(lat) > 90) or (abs(lon) > 180) or (abs(dt) > 1e10): return prevTime
        if not time: return prevTime
        t = time - timedelta(seconds=dt)
        if prevTime is not None and (abs(t - prevTime) <= timedelta(seconds=60)): return prevTime
        with open(ofn, "a") as fp:
            if fp.tell() == 0:
                fp.write("time,lat,lon\n")
            fp.write(f"{time.timestamp():.0f},{lat:.7f},{lon:.7f}\n")

        if self.__sendTo:
            for tgt in self.__sendTo:
                tgt.put(ofn)

        return time

    def __mkSensor(self, name:bytes, units:bytes, val:bytes, dt:bytes, time:datetime,) -> None:
        dt = float(dt)
        if dt > 3000: return

        self.__sensors.put(
                str(name, "utf-8"), 
                str(units, "utf-8"), 
                float(val),
                (time - timedelta(seconds=dt)).timestamp(),
                )

    def runIt(self): # Called on start
        ofn = os.path.join(self.args.csvDir, self.__gliderName + ".csv")
        logging.info("Starting %s", ofn)

        if not os.path.isdir(self.args.csvDir):
            logging.info("Creating %s", self.args.csvDir)
            os.makedirs(self.args.csvDir, mode=0o755, exist_ok=True)

        q = self.__queue

        reLoci = self.__location
        reTime = self.__time
        reSensor = self.__sensor
        reDevices= self.__devices
        reZmodem = self.__zmodem

        t = None
        prevTime = None
        sensors = dict()

        while True:
            line = q.get()
            q.task_done()
            matches = reLoci.match(line)
            if matches:
                prevTime = self.__matchedLocation(matches, t, prevTime, ofn)
                continue

            matches = reTime.match(line)
            if matches:
                t = datetime.strptime(
                        str(matches[1], "utf-8"),
                        "%b %d %H:%M:%S %Y",
                        ).replace(tzinfo=timezone.utc)
                continue

            matches = reSensor.match(line)
            logging.info("SENSORS line %s\nmatches %s", line, matches)
            if matches and t: 
                self.__mkSensor(matches[1], matches[2], matches[3], matches[4], t,)
                continue

            matches = reDevices.match(line)
            logging.info("DEVICES line %s\nmatches %s", line, matches)
            if matches:
                self.__sensors.devices()
                continue

            matches = reZmodem.match(line)
            if matches:
                self.__download.put(datetime.now(tz=timezone.utc) if t is None else t)
                continue

if __name__ == "__main__":
    from TPWUtils import Logger
    from SendTo import SendToTarget

    parser = ArgumentParser()
    parser.add_argument("dialog", type=str, nargs="+", help="Dialog log file(s) to parse")
    parser.add_argument("--glider", type=str, default="probar", help="Name of glider")
    parser.add_argument("--timeout", type=float, default=20,
            help="How long to wait for parsing to complete.")
    Logger.addArgs(parser)
    SendToTarget.addArgs(parser)
    Sensors.addArgs(parser)
    DownloadFiles.addArgs(parser)
    ParseDialog.addArgs(parser)
    args = parser.parse_args()

    Logger.mkLogger(args, logLevel=logging.INFO)

    logging.info("args %s", args)

    sendTo = []
    if args.hostname:
        for tgt in args.hostname:
            sendTo.append(SendToTarget(tgt, args))
            sendTo[-1].start()

    sensors = Sensors(args.glider, args, sendTo)
    sensors.start()

    download = DownloadFiles(args.glider, args, sendTo)
    download.start()
  
    thrd = ParseDialog(args.glider, args, sendTo, sensors, download)
    thrd.start()

    for fn in args.dialog:
        with open(fn, "rb") as fp:
            while True:
                line = fp.readline()
                if not line: break
                thrd.put(line)
    try:
        Thread.waitForException(timeout=args.timeout)
    except:
        logging.exception("Unexpected termination")
