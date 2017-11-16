#!/usr/bin/python

"""Data gather for Quentin"""

import os
import sys
import time
import datetime
import argparse
import logging

from dataaccess import datagather

# Thread timeout
_TIMEOUT_ = 5.0
# Maximum number of threads running at the same time
_MAX_THREADS_ = 10

def gather(exchange, instruments):
    """
    Initialise threads to download data
    """

    threads = [datagather.DataGather(exchange, instrument) for instrument in instruments]

    for t in threads:
        t.start()

    for t in threads:
        t.join(_TIMEOUT_)

    #for idx, t in enumerate(threads):
    #    print "This is ", idx, "|", t.exchange, ":", t.instrument
    #    print "Data:", t.RESULT
        
    return [t.RESULT for t in threads]

if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='Quentin Data Gather Engine',
                                     description='Gathers data from Finance data sources',
                                     epilog='This program might infring data source\'s TOS.')

    parser.add_argument('-x', '--exchange', metavar='exchange', type=str, dest='exchange', required=True, help='Exchange that the instrument can be found')
    parser.add_argument('-i', '--instruments', metavar='instruments', type=str, action='append', dest='instruments', required=True, help='Instrument to query data source')
    parser.add_argument('-v', '--verbosity', action='store_true', default=False, help="The verbosity of the output reporting for the found search results.")
    
    args = parser.parse_args()

    print args
    
    results = gather(args.exchange, args.instruments)

    for result in results:
        print result
