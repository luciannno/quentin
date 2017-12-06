import logging
import pandas as pd
import MySQLdb
import MySQLdb.cursors
import db_config as cfg
from sqlalchemy import create_engine

class DBAccess(object):
    """
    Class for DB access
    @TODO: DB information should be taken from a cfg file or something similiar
    """
    
    _connection = None
    _cursor = None
    logger = None

    def __init__(self):
        hostname = cfg.mysql['host']
        username = cfg.mysql['user']
        password = cfg.mysql['passwd']
        database = cfg.mysql['db']
        self.connection = MySQLdb.connect(hostname, username, password, database) #, cursorclass=MySQLdb.cursors.DictCursor
        self.cursor = self.connection.cursor()
        self.logger = logging.getLogger("quentin")

        #self.engine = create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(username, password, hostname, database))
        
    def __del__(self):
        self.connection.close()
        
    def query(self, query, params=""):
        return self.cursor.execute(query, params)

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, value):
        if isinstance(value, MySQLdb.connections.Connection):
            self._connection = value
    
    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        if isinstance(value, MySQLdb.cursors.Cursor):
            self._cursor = value
    
    def fetchRow(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()

    def fetchAll(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def updateRow(self, table, **data):
        idName = "id"
        id = data[idName]
        del data[idName]
        sets = []
        for key in data.keys():
            sets.append("%s = %%s" % key)
            set = ', '.join(sets)
        
        qq = "UPDATE %s SET %s WHERE %s = %%s" % (table, set, idName,)
        
        try:
            self.cursor.execute(qq, tuple(data.values()+[id]))
            self.connection.commit()
        except:
            self.connection.rollback()
            raise Exception
            
        #print self.cursor._last_executed
        #return self.cursor.lastrowid
        #print self.cursor._last_executed

    def _returnList(self, sql, params=""):
        self.query(sql, params)
        row = self.cursor.fetchall()
        list_key_value = [list(item) for item in row] #[ [k,v] for k, v in row.items() ]
        return list_key_value #list(sum(row, ()))
        
        
    def insertRow(self, table, **data):
        """

        """
        
        keys = ', '.join(data.keys())
        vals = ', '.join(["%s"] * len(data.values()))
        query = "INSERT INTO %s (%s) VALUES (%s)" % (table, keys, vals)

        try:
            self.cursor.execute(query, tuple(data.values()))
            self.connection.commit()
        except:
            self.connection.rollback()
            raise Exception
            
        return self.cursor.lastrowid

    def getAllInstrumentsinExchange(self, exchange):
        """
        Retrieve a list of instruments registered against a single Exchange
        """
        
        sql = """SELECT a.google_symbol, a.id 
        FROM instrument as a inner join exchange as b on a.exchange_id=b.id 
        where b.google_code = %s""";

        return self._returnList(sql, [exchange])
        
    def getAllTickers(self):
        """

        """
        
        self.query("""select i.id, i.google_symbol, i.yahoo_symbol, i.prefered_download, e.time_zone
                                from instrument as i inner join exchange as e
                                on i.exchange_id = e.id""")

        return self._returnList(sql)


    def getMinData(self, instrument_id):
        """
        Retrive Minute Data from Database for a single instrument
        """
        self.query("select * from min_price as i where instrument_id = %s and price_date >= '2016-02-12 20:45:00' order by price_date asc", [ instrument_id ])
        headers =  [n[0] for n in self.cursor.description]
        rows = self.cursor.fetchall() # return list of tuples
        ret = pd.DataFrame(list(rows), columns = headers) #need to get
        
        return ret
    
    def getLatestData(self, instrument_id=None, period=15, limit=300):
        """
        Retrive Data from Database for a single instrument in a given period in minutes
        :param instrument_id: The instrument_id from database
        :param period: The time in minutes to group by. ie: For 15 min data period=15.
        :param limit: Maximum amount of lines returned from DB
        """
        if not instrument_id:
            raise Exception("Instrument id is mandatory");
        
        self.logger.debug("Retrieve Lastest Data for instrument %d" % (instrument_id))
        
        query = """select * from (select id,
                substring_index(group_concat(price_date order by a.id), ',', 1) as price_date,
                substring_index(group_concat(open_price order by a.id), ',', 1) as open_price,
                substring_index(group_concat(close_price order by a.id desc), ',', 1) as close_price,
                max(a.high_price) as high_price, 
                min(a.low_price) as low_price, 
                sum(a.volume) as volume
                from min_price as a
                where a.instrument_id = %s
                group by UNIX_TIMESTAMP(a.price_date) DIV %s
                order by a.price_date desc
                limit %s) as q order by price_date asc"""
        
        self.query(query, [ instrument_id, period*60, limit ])
        self.logger.debug("Query: %s" % (query % (instrument_id, period*60, limit)))
        
        try:
            headers =  [n[0] for n in self.cursor.description]
            rows = self.cursor.fetchall() # return list of tuples
            df = pd.DataFrame(list(rows), columns = headers) #need to get
            df.index = df.price_date
            df = df.drop("price_date", 1)
        except:
            df = None
            
        return df
    
    def getInstrument(self, symbol):

        query = """select i.id, i.symbol, i.google_symbol, i.yahoo_symbol, i.prefered_download, e.time_zone, i.ib_symbol, date_format(i.expiry, '%%Y%%m') as expiry, t.ib_secType, e.world_ex_id
                    from instrument as i
                    inner join exchange as e on i.exchange_id = e.id
                    inner join instrument_type as t on i.instrument_type_id = t.id
                    where i.symbol = %s"""

        self.query(query, [symbol])
        df = None
        
        try:
            rows = self.cursor.fetchall() # return list of tuples
            headers =  [n[0] for n in self.cursor.description]
            df = pd.DataFrame(list(rows), columns=headers)
        except:
            df = None
 
        return df
    
    def getInstrumentFromIBSymbol(self, ib_symbol, expiry=None):

        query = """select i.id, i.symbol, i.google_symbol, i.yahoo_symbol, i.prefered_download, e.time_zone, i.ib_symbol, date_format(i.expiry, '%%Y%%m') as expiry, t.ib_secType, e.world_ex_id
                    from instrument as i
                    inner join exchange as e on i.exchange_id = e.id
                    inner join instrument_type as t on i.instrument_type_id = t.id
                    where i.ib_symbol = %s"""

        params = [ib_symbol]

        if expiry:
            query = query + " and i.expiry = %s"
            params.append(expiry)
        
        self.query(query, params)
        
        try:
            rows = self.cursor.fetchall() # return list of tuples
            headers =  [n[0] for n in self.cursor.description]
            df = pd.DataFrame(list(rows), columns=headers)
        except:
            df = None
 
        return df

    def getPortfolio(self, portfolio_id):
        query = """select p.id, p.name, p.account_id, broker, ext_account
                    from portfolio as p
                    inner join account as a on a.id = p.account_id
                    where p.id = %s and a.active = 1"""

        self.query(query, [portfolio_id])
        rows = None
        
        try:
            rows = self.cursor.fetchone()
        except:
            rows = None
        
        return rows
    
    def getStrategies(self, portfolio_id):
        query = """select s.code
                    from strategy as s
                    where s.portfolio_id = %s and s.auto_activate = 1
                    order by s.run_order"""

        self.query(query, [portfolio_id])
        rows = None
        
        try:
            rows = self.cursor.fetchall()
        except:
            rows = None
        
        return rows

        
