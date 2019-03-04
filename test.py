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
c_totals = defaultdict(long)
u_totals = defaultdict(long)
agg_c_totals = defaultdict(long)
agg_u_totals = defaultdict(long)
MAXINT32 = 2 ** 31

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

    calculate_totals(c_connections,c_totals)
    calculate_totals(u_connections,u_totals)
    aggregate_totals(c_totals, agg_c_totals)
    aggregate_totals(u_totals, agg_u_totals)

#------------------------------------------------------------------------------
def aggregate_totals(cont,totalscont):
    for t in cont:
        (act, id) = t.split(":")
        print ("aggregate_totals act {0} id {1} add {2} bytes".format(act,id,cont[t]))
        totalscont[act] += cont[t]

#------------------------------------------------------------------------------
def calculate_totals(cont,totalscont):
    #sort all events by timestamp
    for i in cont:
        cont[i].sort()
        uuidevents = cont[i]
        bytesmin = None
        bytesmax = None
        print "Events for {0}".format(i)
        for e in uuidevents:
            print " Event: {0}".format(e)
            time = e.time
            nbytes = e.bytes

            # first iteration
            if bytesmin is None:
                print(" assign bytesmin,bytesmax  = {0}".format(nbytes))
                bytesmin = nbytes
                bytesmax = nbytes

            # non-first iteration
            if nbytes > bytesmax:
                print(" nbytes > {0} set bytesmax  = {0}".format(bytesmax,nbytes))
                bytesmax = nbytes
            elif nbytes < bytesmax:
                # most likely integer rollover
                print ("nbytes {0} < bytesmax {1}".format(nbytes, bytesmax))
                ovflw = MAXINT32 - nbytes
                bytesmax = (MAXINT32 - prev) + nbytes
                print "ovflw={0} maxint={1} nbytes={2} prev={3} set bytesmax to {4}\n".format(ovflw, MAXINT32, nbytes,prev, bytesmax)

            print ("set prev to {0}".format(nbytes))
            prev = nbytes

        totalscont[i] = bytesmax - bytesmin
        print("totalscont[i]{0}  = bytesmax {1} - bytesmin {2}".format(totalscont[i], bytesmax,bytesmin))
                
#------------------------------------------------------------------------------
def main():
    processfiles('testdata')

    print ("\nResults:")

    print ('==============\nc connections:\n==============')
    # for c in c_connections:
    #     print("Total bytes: {0}".format(c_totals[c]))
    #     for evt in c_connections[c]:
    #         print "{0} {1}".format(c, evt)
    #     print "-----------------------"
    for t in agg_c_totals:
        print ("{0}: {1}".format(t, agg_c_totals[t]))

    print ("==============\nu connections:\n==============")
    # for u in u_connections:
    #     print("Total bytes: {0}".format(u_totals[u]))
    #     for evt in u_connections[u]:
    #         print "{0} {1}".format(u, evt)
    #     print "-----------------------"

    for t in agg_u_totals:
        print ("{0}: {1}".format(t, agg_u_totals[t]))

#------------------------------------------------------------------------------
if __name__ == "__main__":
    main()


