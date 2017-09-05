# -*- coding: utf-8 -*-

from Queue import Queue, Full, Empty

class Pool(Queue):

    """Manage a fixed-size pool of reusable, identical objects."""

    def __init__(self, constructor, poolsize=5):
        Queue.__init__(self, poolsize)
        self.constructor = constructor

    def get(self, block=1):
        """Get an object from the pool or a new one if empty."""
        try:
            return self.empty() and self.constructor() or Queue.get(self, block)
        except Empty:
            return self.constructor()

    def put(self, obj, block=1):
        """Put an object into the pool if it is not full. The caller must
        not use the object after this."""
        try:
            return self.full() and None or Queue.put(self, obj, block)
        except Full:
            pass

class Constructor:

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return apply(DB, self.args, self.kwargs).conn()
        
def InitPoolDB(AppConfig):
    PoolDB = {}

    for i in AppConfig:
        PoolDB[i] = Pool(Constructor(
             db_type     = AppConfig[i]['db_type']
            ,db_host     = AppConfig[i]['db_host']
            ,db_port     = AppConfig[i]['db_port']
            ,db_name     = AppConfig[i]['db_name']
            ,db_user     = AppConfig[i]['db_user']
            ,db_password = AppConfig[i]['db_password']),20)

    return PoolDB

class DB(object):
    
    def __init__(self,**kwargs):
        self.dbconfig = kwargs

    def conn(self):
        if self.dbconfig['db_type'] == 'mysql':
            from .mysql import Connection as mysql_conn
            db = mysql_conn(
                host='%s:%s' % (self.dbconfig['db_host'], self.dbconfig['db_port']),
                database=self.dbconfig['db_name'],
                user=self.dbconfig['db_user'], 
                password=self.dbconfig['db_password'])

        elif self.dbconfig['db_type'] == 'db2':
            from .db2 import Connection as db2_conn
            db = db2_conn(
                dsn = self.dbconfig['db_name'],
                uid = self.dbconfig['db_user'],
                pwd = self.dbconfig['db_password'])

        else:
            raise Exception("db_type不支持")

        return db