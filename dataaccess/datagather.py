#!/usr/bin/env python

import threading
import time
import pytz
from datetime import datetime
import dataaccess as dt
#from pandas.io import sql

class DataGather(threading.Thread):
    """
    Query data provider engine
    """

    def __init__(self, exchange, instrument):
        """

        :param exchange:
        :param instrument:
        """
        super(DataGather, self).__init__()
        self.exchange      = exchange
        self.instrument    = instrument['google_symbol']
        self.instrument_id = instrument['id']
        
    def run(self):
        """

        :return:
        """

        self.RESULT = {} # Create a result for every thread
        self.RESULT['start'] = time.time()
        self.RESULT['exchange'] = self.exchange
        self.RESULT['instrument'] = self.instrument

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tz_local = pytz.timezone(self.exchange['time_zone'])
        data = dt.get_google_finance_intraday(self.exchange['google_code'], self.instrument)
        data = data.tz_localize('America/Sao_Paulo').tz_convert(tz_local)

        data['instrument_id'] = self.instrument_id
        data['created_date'] = timestamp
        data['last_updated_date'] = timestamp
        
        self.RESULT['data'] = data
        self.RESULT['end'] = time.time()
        self.RESULT['status'] = "Successfully"
