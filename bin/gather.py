#!/usr/bin/python

"""Data gather for Quentin"""

import os
import sys
import time
import datetime
import argparse
import logging
import quentin_config as cfg

import dataaccess as dt

# Thread timeout
_TIMEOUT_ = 5.0
# Maximum number of threads running at the same time
_MAX_THREADS_ = 8

def gather(exchange, instruments):
    """
    Initialise threads to download data
    """

    threads = [dt.datagather.DataGather(exchange, instrument) for instrument in instruments]

    #for t in threads:
    #    t.start()

    #for t in threads:
    #    t.join(_TIMEOUT_)

    thread_total = len(threads)

    for j in range(0, thread_total, _MAX_THREADS_):
        try:
            for i in range(j, j+_MAX_THREADS_, 1):
                #print "Start thread: ", i
                if i < thread_total:
                    threads[i].start()
                else:
                    #raise Exception("Not in thread list")
                    break
        except Exception as e:
            continue

        try:
            for i in range(j, j+_MAX_THREADS_, 1):
                #print "Join thread: ", i
                if i < thread_total:
                    threads[i].join(_TIMEOUT_)
                else:
                    #raise Exception("Not in thread list")
                    break
        except Exception as e:
            continue

    return [t.RESULT for t in threads]


if __name__ == '__main__':


    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create a file handler
    handler = logging.FileHandler(cfg.quentin['gather_log'])
    handler.setLevel(logging.DEBUG)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)

    exchange    = None
    instruments = None
    
    parser = argparse.ArgumentParser(prog='Quentin Data Gather Engine',
                                     description='Gathers data from Finance data sources',
                                     epilog='This program might infring data source\'s TOS.')

    parser.add_argument('-x', '--exchange', metavar='exchange', type=str, dest='exchange', required=True, help='Exchange that the instrument can be found')
    parser.add_argument('-i', '--instruments', metavar='instruments', type=str, action='append', dest='instruments', required=False, help='Instrument to query data source')
    parser.add_argument('-o', '--output', metavar='output', type=str, dest='output', required=False, help='Set output directory')
    parser.add_argument('-v', '--verbosity', action='store_true', default=False, help="The verbosity of the output reporting for the found search results.")
    parser.add_argument('-s', '--save', action='store_true', default=False, help="Save all results in database")

    args = parser.parse_args()

    db = dt.DBAccess()
    
    if args.exchange and args.instruments:
        #exchange = args.exchange
        #instruments = args.instruments
        pass

    elif args.exchange:
        exchange = db.getExchange(world_ex_id=args.exchange)[0]
        instruments = db.getAllInstrumentsinExchange(exchange['world_ex_id'])

    logger.debug('Exchange: {}'.format(exchange['world_ex_id']))

    #print [i[0] for i in instruments]
        
    if exchange and instruments:

        #p_queue = zip(instruments, instruments[1:], instruments[2:], instruments[3:])

        results = gather(exchange, instruments)

        for result in results:
            
            st = datetime.datetime.fromtimestamp(result['start']).strftime('%Y-%m-%d %H:%M:%S')
            en = datetime.datetime.fromtimestamp(result['end']).strftime('%Y-%m-%d %H:%M:%S')

            if args.output:
                file      = "{}_{}.txt".format(result['exchange'], result['instrument'])
                folder    = args.output
                full_path = os.path.join(folder, file)
                result['data'].to_json(full_path)

            #print result

            #try:
            #    result['data'].to_sql(con=self.db.engine, name='min_price', if_exists='append', index=False)
            #except Exception, e:
            #    print 'Failed to mysql' + str(e)

            frame = result['data']
            frame.reset_index(level=0, inplace=True)    
            wildcards = ','.join(['%s'] * len(frame.columns))
            cols=[k for k in frame.dtypes.index]
            colnames = ','.join(cols)
            insert_sql = 'INSERT IGNORE INTO %s (%s) VALUES (%s)' % ('min_price', colnames, wildcards)
            data = [tuple(x) for x in frame.values]

            #print data
            
            #print insert_sql
            
            #result['data'].reset_index(level=0, inplace=True) #['price_date'] = result['data'].price_date
            #data_dict = result['data'].index.tolist()

            #print data_dict[0]
            
            #print result['data'].columns.values.tolist()
            #print result['data'].index.name

            db.cursor.executemany(insert_sql, data)
            logger.info("Instrument: {} : {} | Start: {} | End: {}".format(result['exchange'], result['instrument'], st, en))
            logger.debug('Rows affected: {}'.format(db.cursor.rowcount))
            
            db.connection.commit()
            
