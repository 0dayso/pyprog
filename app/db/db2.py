# -*- coding: utf-8 -*-
import DB2
import itertools

class Row(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

class Connection(object):
    def __init__ (self,dsn,uid,pwd):
        self._dsn = dsn
        self._uid = uid
        self._pwd = pwd
        self._db = None
        try:
            self.reconnect()
        except Exception:
            print 'self.reconnect() error'
        

    def __del__(self):
        self.close()

    def close(self):
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        self.close()
        self._db = DB2.connect(dsn=self._dsn, uid=self._uid, pwd=self._pwd)

    def query(self, query, *parameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0].lower() for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
            
    def get(self, query, *parameters):
        """Returns the first row returned for the given query."""
        rows = self.query(query, *parameters)
        if not rows:
            return None
        elif len(rows) > 1:
            raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    def execute_rowcount(self, query, *parameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            self._db.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    def execute(self, query, *parameters):
        return self.execute_rowcount(query, *parameters)

    def _cursor(self):
        # if self._db is None:
        #     self.reconnect()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters):
        return cursor.execute(query, parameters)