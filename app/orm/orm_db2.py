# -*- coding: utf-8 -*-
from app import PoolDB

class SafeDB(object):

    """ Class doc """
    def __init__ (self,source='default'):
        self.source = source
        self._db = PoolDB[self.source].get()
        self._exec_lst_= [] #执行列表参数

    def __enter__(self):
        # print "%s获取数据库句柄" % os.getpid()
        return self._db

    def __exit__(self ,exc_type, exc_val, exc_tb):
        #没有出现异常则还给连接池，出现异常则关闭句柄
        if exc_type==None and exc_val==None and exc_tb==None :
            PoolDB[self.source].put(self._db)
            # print "%s返回数据库句柄" % os.getpid()
        else:
            if getattr(self._db, "close", None) is not None:
                self._db.close()
                # print "%s关闭数据库句柄" % os.getpid()
                
    # def __del__(self):
    #     if self._db <> None :
    #         self._db.close()

class BaseOrm:
    __tablename__= 'BASE_ORM'
    __primary__ = None
    __source__ = 'default'
    __translKv__ = None #接收值为 ('K','V')

    def __init__ (self,db=None):
        self._sql = ''
        self._type = ''
        self._para = []
        self._sql_kv = {}
        self._db = db

    def getbysql(self,*args):
        with SafeDB(self.__source__) as _db:
            return apply(_db.get,args)

    def querybysql(self,*args):
        with SafeDB(self.__source__) as _db:
            return apply(_db.query,args)

    def excutebysql(self,*args):
        with SafeDB(self.__source__) as _db:
            return apply(_db.execute_rowcount,args)

    def insertbysql(self,*args):
        with SafeDB(self.__source__) as _db:
            return apply(_db.execute_rowcount,args)

    def select (self,params={},**kwargs):
        self._type = 's'; self._sql = ''; self._para = []; self._sql_kv = {}
        _dct = dict(params,**kwargs)
        self._sql += """select * from %s """ % self.__tablename__
        return self.where(params=_dct)

    def update (self,params={},**kwargs):
        self._type = 'u'; self._sql = ''; self._para = []; self._sql_kv = {}
        _dct = dict(params,**kwargs)
        self._sql_kv['update'] = _dct
        _lst = _dct.keys()
        self._sql += """update %s set """ % self.__tablename__ + ','.join(["%s=?" % i for i in _lst]) + " "
        for i in _lst: self._para.append(_dct[i])
        return self

    def insert (self,params={},**kwargs):
        self._type = 'i'; self._sql = ''; self._para = []; self._sql_kv = {}
        _dct = dict(params,**kwargs)
        self._sql_kv['insert'] = _dct
        _lst = _dct.keys()
        self._sql += """insert into %s(""" % self.__tablename__ \
                   + ','.join(["%s" % i for i in _lst]) + ") values(" \
                   + ','.join(['?' for i in _lst]) + ") "
        for i in _lst: self._para.append(_dct[i])
        return self

    def delete (self,params={},**kwargs):
        self._type = 'd'; self._sql = ''; self._para = []; self._sql_kv = {}
        _dct = dict(params,**kwargs)
        self._sql += """delete from %s """ % self.__tablename__
        return self.where(params=_dct)

    def limit (self,args):
        self._sql += """ fetch first %s rows only """ % args
        # self._para.append(args)
        return self

    def where (self,params={},**kwargs):
        _dct = dict(params,**kwargs)
        self._sql_kv['where'] = _dct
        if len(_dct)==0: pass
        else:
            _lst = _dct.keys()
            self._sql += " where " + ' and '.join(["%s = ?" % i for i in _lst]) + " "
            for i in _lst:
                self._para.append(_dct[i])
        return self

    def _chk_where(self):
        """确认条件里面是否有where条件"""
        if ' where ' in self._sql: self._sql += ' and '
        else: self._sql += ' where '

    def in_(self,col,args):
        """col   列名
           args  绑定变量值 类型为 list
        """
        self._chk_where()
        _in_k = ','.join(['?' for i in args])
        self._sql += """ %s in (%s) """ % (col.lower(), _in_k)
        for i in args: self._para.append(i)
        return self

    def where2(self,wheresql,*args):
        self._chk_where()
        self._sql += """ %s """ % wheresql
        if len(args)==0: pass
        else:
            for i in args: self._para.append(i)
        return self

    def order (self,col=''):
        _order = []
        _sql_order =''
        for i in col.split(','):
            for j in i.split(' '):
                if j.strip().upper() in ['DESC','ASC']:
                    _order.append(j)
                elif j.strip()=='':pass
                else:
                    _order.append(',%s' % j)
        _sql_order = ' '.join(_order)[1:]
        self._sql += """ order by %s """ % _sql_order
        return self

    def echo (self):
        _msg = "sql:%s | para:%s" % (self._sql,str(tuple(self._para)))
        print _msg
        return _msg

    def count(self,col='' ):
        if col=='':
            _col = '*'
        else:
            _col = "%s" % col
        self._sql = 'select count(%s) cnt_1 from (%s) a' % (_col,self._sql)
        return self.get()['cnt_1']

    def sum(self,col ):
        self._sql = 'select sum(%s) sum_1 from (%s) a' % (col,self._sql)
        return self.get()['sum_1']

    def max(self,col):
        self._sql = 'select max(%s) max_1 from (%s) a' % (col,self._sql)
        return self.get()['max_1']

    def _chk_whthur(self):
        """确认条件里面是否有where条件"""
        if ' with ur' in self._sql: pass
        else: self._sql += ' with ur'

    def query (self):
        self._chk_whthur()
        if self._db == None:
            with SafeDB(self.__source__) as db:
                return apply(db.query,tuple([self._sql] + self._para))
        else:
            return apply(self._db.query,tuple([self._sql] + self._para))

    def get (self):
        self._chk_whthur()
        if self._db == None:
            with SafeDB(self.__source__) as db:
                return apply(db.get,tuple([self._sql] + self._para))
        else:
            return apply(self._db.get,tuple([self._sql] + self._para))

    def execute (self):
        self._chk_whthur()
        if self._db == None:
            with SafeDB(self.__source__) as db:
                return apply(db.execute,tuple([self._sql] + self._para))
        else:
            return apply(self._db.execute,tuple([self._sql] + self._para))

    def execute_rowcount (self):
        self._chk_whthur()
        if self._db == None:
            with SafeDB(self.__source__) as db:
                return apply(db.execute_rowcount,tuple([self._sql] + self._para))
        else:
            return apply(self._db.execute_rowcount,tuple([self._sql] + self._para))
