
import threading
import time
from datetime import datetime
import dataaccess as dt
from pandas.io import sql

class DataGather(threading.Thread):
    """
    Query data provider engine
    """

    def __init__(self, exchange, instrument):
        super(DataGather, self).__init__()
        self.exchange      = exchange['google_code']
        self.instrument    = instrument['google_symbol']
        self.instrument_id = instrument['id']
        
    def run(self):
        #print "Running...", self.name
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        data = dt.get_google_finance_intraday(self.exchange, self.instrument)
        
        self.db = dt.DBAccess()

        data['instrument_id'] = self.instrument_id        
        data['created_date'] = timestamp
        data['last_updated_date'] = timestamp
        
        self.RESULT = {} # Create a result for every thread
        self.RESULT['exchange'] = self.exchange
        self.RESULT['instrument'] = self.instrument
        self.RESULT['start'] = time.time()
        self.RESULT['data'] = data
        self.RESULT['end'] = time.time()
        self.RESULT['status'] = "Successfully"

        #print data
        #Index([u'Open', u'High', u'Low', u'Close', u'Volume'],
                
        #sql.write_frame(data, con=self.db.connection, name='min_price', if_exists='append', flavor='mysql')
        #try:
        #    data.to_sql(con=self.db.engine, name='min_price', if_exists='append', index=False)
        #except Exception, e:
        #    print 'Failed to mysql' + str(e)
        #print self.db.connection
        
