import copy
import logging
import time

try:
    import psycopg2
except ImportError:
    psycopg2=None
    raise ImportError("请安装psycopg2")

try:
    from itertools import izip as zip
except ImportError:
    pass

class Connection():
    """
    对 psycopg2 进行简单的封装，
    让 postgre 用起来像 tornado 的默认支持的 mysql 
    """
    def __init__(self,host,database,user=None,password=None):
        """
        host -> sql server machine address,like 127.0.0.1[:5432]
        database -> database name
        user -> database username
        password -> password
        """
        self.host=host
        self.database=database
        args=dict(database=database)
        if user is None:
            args['user']='postgres' #by default
        else:
            args['user']=user
        if password is not None:
            args['password']=password
        pair=host.split(':')
        args['host']=pair[0]
        if len(pair) == 2:
            args['port']=pair[1]
        else:
            args['port']=5432

        self._conn=None
        self._conn_args=args
        try:
            self.reconnect()
        except Exception:
            logging.error("Cannot connect to postgres on %s",self.host,
                            exc_info=True)
    def __del__(self):
        self.close()

    def close(self):
        """ Closes this database connection."""
        if getattr(self,'_conn',None) is not None:
            self._conn.close()
            self._conn=None

    def reconnect(self):
        """Closes the existing database connection and reopen it"""
        self.close()
        self._conn=psycopg2.connect(**self._conn_args)
        self._conn.autocommit=True

    def iter(self,query,*parameters):
        cursor=self._cursor()
        try:
            self._execute(cursor,query,parameters)
            column_names=[ d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names,row))
        finally:
            cursor.close()

    def query(self,query,*parameters):
        cursor=self._cursor()
        try:
            self._execute(cursor,query,parameters)
            column_names=[ d[0] for d in cursor.description]
            return [Row(zip(column_names,row)) for row in cursor]
        finally:
            cursor.close()

    def get(self,query,*parameters):
        rows=self.query(query,*parameters)
        if not rows:
            return None
        elif len(rows)>1:
            raise Exception("Multiple rows returned for get query")
        else:
            return rows[0]

    def execute(self,query,*parameters):
        return self.execute_lastrowid(query,*parameters)

    def execute_lastrowid(self,query,*parameters):
        cursor=self._cursor()
        try:
            self._execute(cursor,query,parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self,query,*parameters):
        cursor=self._cursor()
        try:
            self._execute(cursor,query,parameters)
            return cursor.rowcount
        finally:
            cursor.close()



    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def _cursor(self):
        return self._conn.cursor()


    def _execute(self,cursor,query,parameters):
        try:
            return cursor.execute(query,parameters)
        except psycopg2.OperationalError:
            logging.error("Error connecting to postgre on %s",self.host)
            self.close()
            raise



class Row(dict):
    def __getattr__(self,name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
                
