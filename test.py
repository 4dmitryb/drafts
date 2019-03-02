#!/usr/bin/env python

# import time
import datetime

def parsetime(val):
    try:
        mobj = datetime.datetime.strptime(val, '%Y:%m:%d_%H:%M:%S')
        return int(mobj.strftime('%s'))
    except ValueError:
        return None

lines = []
lines.append('2019:03:01_22:08:28')
lines.append('2019:03:01_22:08:29')
lines.append('2019:03:01_22:08:59')
lines.append('2019:03:01_22:08:60')

for l in lines:
    result = parsetime(l)
    if result:
        print result


# print parsetime('2019:03:01_22:08:29')
# print parsetime('2019:03:01_22:08:59')
# print parsetime('2019:03:01_22:08:60')


# time,connection,compBytesSent,compSentRate,uncompbBytesSent,uncompByteRate,compression
# 2019:03:01_22:08:28,SOMEUUID:33,59,0,0,180035,0,0
