
import threading
import time
import dataaccess

class DataGather(threading.Thread):
    """
    Query data provider engine
    """

    def __init__(self, exchange, instrument):
        super(DataGather, self).__init__()
        self.exchange   = exchange
        self.instrument = instrument
        
    def run(self):
        print "Running...", self.name

        self.RESULT = {}
        self.RESULT['data'] = dataaccess.get_google_finance_intraday(self.exchange, self.instrument)
