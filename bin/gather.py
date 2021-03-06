#!/usr/bin/python

"""Data gather for Quentin"""

import os
import sys
import time
import datetime
import threading
import quentin
import dataaccess

def main_loop():

    print "Test time:", datetime.datetime.time(datetime.datetime.now())


if __name__ == '__main__':

    t = quentin.text()
    d = dataaccess.YahooAccess()

    try:

        print d.MakeYahooURL("test")        

        t.do_text()
        now = datetime.datetime.now()
        run_at = now + datetime.timedelta(seconds=0)
        delay = (run_at - now).total_seconds()
        run_at = now.replace(day=now.day, hour=17, minute=0, second=0, microsecond=0)
        delta_time = run_at - now

        #main_loop()

        print "Now:", now
        print "Run at:", run_at
        print "Delta:", delta_time
 
        #t = threading.Timer(delta_time.seconds+1, main_loop)
        #t.start()

    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)      
