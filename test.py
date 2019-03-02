#!/usr/bin/env python

########################################################################################
# 2019
# Author: Dmitry Belyavsky
# 
#
########################################################################################


from datetime import datetime
from collections import defaultdict
from os import listdir
from os.path import isfile,join
from collections import namedtuple

#------------------------------------------------------------------------------
# globals
#------------------------------------------------------------------------------

timestamp_format = '%Y:%m:%d_%H:%M:%S'
c_connections = defaultdict(list)
u_connections = defaultdict(list)
c_totals = defaultdict(int)
u_totals = defaultdict(int)
agg_c_totals = {}
agg_u_totals = {}

Event = namedtuple('Event', 'time bytes rate')

#------------------------------------------------------------------------------
def parsetimestamp(val):
    try:
        return int((datetime.strptime(val, timestamp_format)).strftime('%s'))
    except:
        return None

#------------------------------------------------------------------------------
def getcondata(val, first_line):
    try:
        val.rstrip()

        #ignore commented lines
        if val[0] is '#':
            return None

        # print ("Parsing line: " + val)
        (tstmp,connid,cbytes,crate,ubytes,urate,cmp) = val.split(',')
        t = parsetimestamp(tstmp)

        if t is None:
            raise ValueError("Invalid timestamp '{0}'".format(tstmp))

        if cmp is '0':
            obj = Event(int(t),int(ubytes),int(urate))
            u_connections[connid].append(obj)
        elif cmp is '1':
            obj = Event(int(t),int(cbytes),int(crate))
            c_connections[connid].append(obj)
        else:
            raise ValueError('Unknown cm type {0}'.format(cmp))
                
        return 1

    except Exception as err:
        if not first_line:
            print ("WARNING: Skipping line [{0}]: {1}").format(val,err)
        return None

#------------------------------------------------------------------------------
def processfile(fname):
    lines = [line.rstrip('\n') for line in open(fname)]

    first_line = True
    for l in lines:
        getcondata(l, first_line)
        first_line = False

#------------------------------------------------------------------------------
def processfiles(path):
    fnames = [ f for f in listdir(path) if isfile(join(path, f))]
    for f in fnames:
        processfile(join(path,f))

    # calculate_totals(c_connections,c_totals)
    calculate_totals(u_connections,u_totals)

#------------------------------------------------------------------------------
def calculate_totals(cont,totalscont):
    #sort all events by timestamp
    for i in cont:
        cont[i].sort()
        uuidevents = cont[i]
        totals = totalscont[i]
        for e in uuidevents:
            print e
            time = e.time
            nbytes = e.bytes
            print ("Add {0} bytes to totals {1}".format(nbytes,totals))
            totals = totals + nbytes
        
#------------------------------------------------------------------------------
def main():
    processfiles('testdata')

    print ("\nResults:")

    # print ('==============\nc connections:\n==============')
    # for c in c_connections:
    #     print("Total bytes: {0}".format(c_totals[c]))
    #     for evt in c_connections[c]:
    #         print "{0} {1}".format(c, evt)
    #     print "-----------------------"

    print ("==============\nu connections:\n==============")
    for u in u_connections:
        print("Total bytes: {0}".format(u_totals[u]))
        for evt in u_connections[u]:
            print "{0} {1}".format(u, evt)
        print "-----------------------"
        

#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()


