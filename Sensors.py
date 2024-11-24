#! /usr/bin/env python3
#
# Aggregate the sensors from the dialog and spit out
#
# This is a rewrite of my existing code for handling SFMC's API
#
# Nov-2024, Pat Welch, pat@mousebrains.com

from argparse import ArgumentParser
import logging
import queue
import os
import numpy as np
from netCDF4 import Dataset
import time
from datetime import datetime, timezone, timedelta
from TPWUtils.Thread import Thread
from SendTo import SendToTarget

class Sensors(Thread):
    def __init__(self, glider:str, args:ArgumentParser, sendTo:SendToTarget) -> None:
        Thread.__init__(self, "SN:" + glider, args)
        self.__gliderName = glider
        self.__sendTo = sendTo
        self.__queue = queue.Queue()
        self.__sensors = dict()

    @staticmethod
    def addArgs(parser:ArgumentParser) -> ArgumentParser:
        grp = parser.add_argument_group(description="Dialog Sensor options")
        grp.add_argument("--sensorDir", type=str, default="./sensors", 
                help="Where to write sensor files to")

    def put(self, name:str, units:str, val:str, time:float) -> None:
        self.__queue.put((name, units, val, time))

    def devices(self):
        self.__queue.put((None, None, None, None))

    def __dump(self, ofn:str) -> None:
        sensors = self.__sensors

        if not sensors: return

        times = []
        for name in sensors:
            times.append(sensors[name][-1])
     
        time = np.median(times)

        with Dataset(ofn, "a" if os.path.isfile(ofn) else "w", format="NETCDF4") as nc:
            if "time" not in nc.dimensions:
                nc.createDimension("time")
                var = nc.createVariable("time", "f8", ("time",))
                var.setncattr("units", "seconds since 1970-01-01")
            index = len(nc.dimensions["time"])
            nc["time"][index] = time
            for name in sensors:
                if name not in nc.variables:
                    var = nc.createVariable(name, "f4", ("time",))
                    var.setncattr("units", sensors[name][0])
                nc[name][index] = sensors[name][1]
        if self.__sendTo:
            for tgt in self.__sendTo:
                tgt.put(self.args.sensorDir)
        return 

    def runIt(self): # Called on start
        ofn = os.path.join(self.args.sensorDir, self.__gliderName + ".sensors.nc")
        logging.info("Starting %s", ofn)

        if not os.path.isdir(self.args.sensorDir):
            logging.info("Creating %s", self.args.sensorDir)
            os.makedirs(self.args.sensorDir, mode=0o755, exist_ok=True)

        q = self.__queue

        while True:
            (name, units, val, t) = q.get()
            if name is None:
                self.__dump(ofn)
            else:
                self.__sensors[name] = (units, val, t)
