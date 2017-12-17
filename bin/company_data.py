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
    else:
        logger.debug("No exchange and instrument provided")
        sys.exit(1)

    logger.debug('Exchange: {}'.format(exchange))

    try:
        exchange = db.getExchange(world_ex_id=exchange)[0]
    except:
        logger.debug("Exchange was not found")
        exit(1)

    if (exchange['world_ex_id'] == 'NASDAQ' or exchange['yahoo_code']) and instruments:

        for instrument in instruments:
            if exchange['world_ex_id'] == "NASDAQ":
                instrumentUrl = 'https://finance.yahoo.com/quote/{0}?p={0}'.format(instrument)
            else:
                instrumentUrl = 'https://finance.yahoo.com/quote/{0}.{1}?p={0}.{1}'.format(instrument, exchange['yahoo_code'])

            try:
                instrumentPage = urlopen(instrumentUrl)
                soup = BeautifulSoup(instrumentPage, "lxml")
                data = soup.findAll("script")[23].string
                p = re.compile(r"root.App.main = (.*);")
                result = p.findall(data.string)
                d = json.loads(result[0])
            except:
                logger.debug(instrumentUrl)
                logger.debug("Quote data not found. {}.{}".format(instrument, exchange['world_ex_id']))
                continue

            quoteSummaryStore = d['context']['dispatcher']['stores']['QuoteSummaryStore']
            price = quoteSummaryStore['price']
            summaryProfile = quoteSummaryStore['summaryProfile']
            quoteType = quoteSummaryStore['quoteType']

            #print json.dumps(quoteSummaryStore, indent=2)

            company = {'id' : None,
                       'name' : price['longName']}

            try:
                company.update({'sector': summaryProfile['sector']})
            except KeyError:
                company.update({'sector': ""})

            try:
                company.update({'industry': summaryProfile['industry']})
            except KeyError:
                company.update({'industry': ""})

            try:

                company['id'] = db.insertRow(table="company",
                                             name=company['name'],
                                             sector=company['sector'],
                                             industry=company['industry'])

            except:

                company = db.getCompany(name=price['longName'])[0]

            instrument = {'id': None,
                          'instrument_type_id': None,
                          'company_id': company['id'],
                          'symbol': None,
                          'exchange_id': None,
                          'yahoo_symbol': None,
                          'google_symbol': None,
                          'prefered_download': 'yahoo_symbol'}

            if quoteType['quoteType'] == "EQUITY":
                instrument['instrument_type_id'] = 3

            #print exchange

            instrument['symbol'] = price['symbol'].split(".")[0]
            instrument['yahoo_symbol'] = price['symbol'].split(".")[0]
            instrument['google_symbol'] = price['symbol'].split(".")[0]
            instrument['currency'] = price['currency']
            instrument['exchange_id'] = exchange['id']

            #print instrument

            try:

                db.insertRow(table="instrument",
                             instrument_type_id=instrument['instrument_type_id'],
                             company_id=instrument['company_id'],
                             symbol=instrument['symbol'],
                             exchange_id=exchange['id'],
                             yahoo_symbol=instrument['yahoo_symbol'],
                             google_symbol=instrument['google_symbol'],
                             prefered_download=instrument['prefered_download'])

            except:
                print "Insert instrument failed {} {}".format(instrument['symbol'], exchange['id'])

    else:
        print "Exchange not found or instruments not given."
