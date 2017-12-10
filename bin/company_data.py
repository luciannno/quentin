#!/usr/bin/python

import os
import sys
import time
import datetime
from bs4 import BeautifulSoup
from urllib import urlopen
import json
import re
import argparse
import logging
import quentin_config as cfg
import dataaccess as dt

if __name__ == '__main__':


    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create a file handler
    handler = logging.FileHandler(cfg.quentin['company_log'])
    handler.setLevel(logging.DEBUG)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(handler)

    exchange    = None
    instruments = None

    parser = argparse.ArgumentParser(prog='Quentin Company Data Gather Engine',
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
        exchange = args.exchange
        instruments = args.instruments

    elif args.exchange:
        pass
        #exchange = args.exchange
        #instruments = db.getAllInstrumentsinExchange(args.exchange)

    logger.debug('Exchange: {}'.format(exchange))

    #print instruments[0]
    #print [i[0] for i in instruments]

    if exchange and instruments:

        for instrument in instruments:

            instrumentUrl = 'https://finance.yahoo.com/quote/{0}.{1}?p={0}.{1}'.format(instrument, exchange)

            try:
                instrumentPage = urlopen(instrumentUrl)
                soup = BeautifulSoup(instrumentPage, "lxml")
                data = soup.findAll("script")[23].string
                p = re.compile(r"root.App.main = (.*);")
                result = p.findall(data.string)
                d = json.loads(result[0])
            except:
                print "Quote data not found. {}.{}".format(instrument, exchange)
                continue

            price = d['context']['dispatcher']['stores']['QuoteSummaryStore']['price']
            summaryProfile = d['context']['dispatcher']['stores']['QuoteSummaryStore']['summaryProfile']

            #company = {'name': price['longName'], 'sector': summaryProfile['sector'], 'industry': summaryProfile['industry']}
            #print "Name:     {}".format(price['longName'])
            #print "Symbol:   {}".format(price['symbol'])
            #print "Sector:   {}".format(summaryProfile['sector'])
            #print "Industry: {}".format(summaryProfile['industry'])

            try:

                id = db.insertRow(table="company",
                                  name=price['longName'], sector=summaryProfile['sector'], industry=summaryProfile['industry'])

            except:

                id = db.getCompany(name=price['longName'])

            print id
