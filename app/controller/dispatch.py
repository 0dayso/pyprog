# -*- coding: utf-8 -*-
import datetime
from ..models import ZbDef,ExDef,TaskConfig,RunDispatch
from app import logging

class Dispatch(object):
    def __init__(self,taskid,gbascode,force=0,debugger=False):
        self._taskid = taskid
        self._gbascode = gbascode
        self._force = force
        self._debugger = debugger
        _inst_RunDispatch = RunDispatch().select(etl_cycle_id=self._taskid)
        logging.Debug("Dispatch,批次%s,获取已发布列表sql:%s" % (self._taskid,_inst_RunDispatch.echo()))
        self._except = [_d_rd['gbas_code'] for _d_rd in _inst_RunDispatch.query()]
        # logging.Debug("Dispatch,批次%s,已发布列表无需重新发布,%s" % (self._taskid,self._except))
        

    def disAll(self):
        if len(self._taskid) == 8:
            self.disDailyZb()
            self.disDailyEx()
        elif len(self._taskid) == 6:
            self.disMonthlyZb()
            self.disMonthlyEx()

    def disZb(self):
        if len(self._taskid) == 8:
            self.disDailyZb()
        elif len(self._taskid) == 6:
            self.disMonthlyZb()

    def disEx(self):
        if len(self._taskid) == 8:
            self.disDailyEx()
        elif len(self._taskid) == 6:
            self.disMonthlyEx()

    def disDailyZb(self):
        """日指标发布"""
        """self._taskid:20170726
           _taskid_datetime:2017-07-26 00:00:00
           _condi_date:2017-07-26"""
        _taskid_datetime = datetime.datetime.strptime(self._taskid,'%Y%m%d')
        _condi_date=datetime.date.isoformat(_taskid_datetime)
        if self._gbascode == None:
            _inst_ZbDef=ZbDef().select(cycle='daily',status='1').where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
        else:
            _arr_gbascode = self._gbascode.split(';')
            _inst_ZbDef=ZbDef().select(cycle='daily',status='1').in_('zb_code',_arr_gbascode).where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
        logging.Debug("disDailyZb,批次%s,获取指标sql:%s" % (self._taskid,_inst_ZbDef.echo()))
        _dct_ZbDef=_inst_ZbDef.query()
        # 已发布的指标是否排除
        if self._force==1:
            _arr_Zb_Except=[]
        else:
            _arr_Zb_Except=self._except
        for i in _dct_ZbDef:
            _expect_end_time = _taskid_datetime+datetime.timedelta(days=1,hours=float(i['expect_end_time'])) if i['expect_end_time'] else _taskid_datetime+datetime.timedelta(days=1)
            _d_rd = {
                    'type'            : 'zb',
                    'gbas_code'       : i['zb_code'],
                    'boi_code'        : i['boi_code'],
                    'cycle'           : 'daily',
                    'etl_cycle_id'    : self._taskid,
                    'etl_status'      : '0',
                    'depend_type'     : i['depend_type'],
                    'proc_depend'     : i['proc_depend'],
                    'gbas_depend'     : i['gbas_depend'],
                    'priority'        : i['priority'],
                    'expect_end_time' : str(_expect_end_time),
                    'dispatch_time'   : str(datetime.datetime.now())
                    }
            if i['zb_code'] in _arr_Zb_Except:
                logging.Debug("disDailyZb,批次%s,排除已发布zb_code:%s" % (self._taskid,i['zb_code']))
            else:
                try:
                    RunDispatch().insert(_d_rd).execute_rowcount()
                except Exception, e:
                    logging.Alarm("disDailyZb,批次%s,发布失败,可能主键错误,zb_code:%s,%s,sql:%s" % (self._taskid,i['zb_code'],str(e),RunDispatch().insert(_d_rd).echo()))
                

    # def disDailyRule(self):
    #     """日规则发布"""
    #     """self._taskid:20170726
    #        _taskid_datetime:2017-07-26 00:00:00
    #        _condi_date:2017-07-26"""
    #     _taskid_datetime = datetime.datetime.strptime(self._taskid,'%Y%m%d')
    #     _condi_date=datetime.date.isoformat(_taskid_datetime)
    #     if self._gbascode == None:
    #         _inst_RuleDef=RuleDef().select(cycle='daily',status='1').where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
    #     else:
    #         _arr_gbascode = self._gbascode.split(';')
    #         _inst_RuleDef=RuleDef().select(cycle='daily',status='1').in_('rule_code',_arr_gbascode).where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
    #     logging.Debug("disDailyRule,批次%s,获取指标sql:%s" % (self._taskid,_inst_RuleDef.echo()))
    #     _dct_RuleDef=_inst_RuleDef.query()
    #     # 已发布的指标是否排除
    #     if self._force==1:
    #         _arr_Rule_Except=[]
    #     else:
    #         _arr_Rule_Except=self._except
    #     for i in _dct_RuleDef:
    #         _expect_end_time = _taskid_datetime+datetime.timedelta(days=1,hours=float(i['expect_end_time']))
    #         _d_rd = {
    #                 'type'            : 'rule',
    #                 'gbas_code'       : i['rule_code'],
    #                 'cycle'           : 'daily',
    #                 'etl_cycle_id'    : self._taskid,
    #                 'etl_status'      : '0',
    #                 'proc_depend'     : i['proc_depend'],
    #                 'gbas_depend'     : i['gbas_depend'],
    #                 'priority'        : i['priority'],
    #                 'expect_end_time' : str(_expect_end_time),
    #                 'dispatch_time'   : str(datetime.datetime.now())
    #                 }
    #         if i['rule_code'] in _arr_Rule_Except:
    #             logging.Debug("disDailyRule,批次%s,排除已发布rule_code:%s" % (self._taskid,i['rule_code']))
    #         else:
    #             try:
    #                 RunDispatch().insert(_d_rd).execute_rowcount()
    #             except Exception, e:
    #                 logging.Alarm("disDailyRule,批次%s,发布失败,可能主键错误,rule_code:%s,%s,sql:%s" % (self._taskid,i['rule_code'],str(e),RunDispatch().insert(_d_rd).echo()))

    def disDailyEx(self):
        """日指标发布"""
        """self._taskid:20170726
           _taskid_datetime:2017-07-26 00:00:00
           _condi_date:2017-07-26"""
        _taskid_datetime = datetime.datetime.strptime(self._taskid,'%Y%m%d')
        _condi_date=datetime.date.isoformat(_taskid_datetime)
        if self._gbascode == None:
            _inst_ExDef=ExDef().select(cycle='daily',status='1').where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
        else:
            _arr_gbascode = self._gbascode.split(';')
            _inst_ExDef=ExDef().select(cycle='daily',status='1').in_('ex_code',_arr_gbascode).where2('online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
        logging.Debug("disDailyEx,批次%s,获取接口sql:%s" % (self._taskid,_inst_ExDef.echo()))
        _dct_ExDef=_inst_ExDef.query()
        # 已发布的指标是否排除
        if self._force==1:
            _arr_Ex_Except=[]
        else:
            _arr_Ex_Except=self._except
        for i in _dct_ExDef:
            _expect_end_time = _taskid_datetime+datetime.timedelta(days=1,hours=float(i['expect_end_time'])) if i['expect_end_time'] else _taskid_datetime+datetime.timedelta(days=1)
            _d_rd = {
                    'type'            : 'exp',
                    'gbas_code'       : i['ex_code'],
                    'boi_code'        : i['boi_code'],
                    'cycle'           : 'daily',
                    'etl_cycle_id'    : self._taskid,
                    'etl_status'      : '0',
                    'depend_type'     : i['depend_type'],
                    'proc_depend'     : i['proc_depend'],
                    'gbas_depend'     : i['gbas_depend'],
                    'priority'        : i['priority'],
                    'expect_end_time' : str(_expect_end_time),
                    'dispatch_time'   : str(datetime.datetime.now())
                    }
            if i['ex_code'] in _arr_Ex_Except:
                logging.Debug("disDailyEx,批次%s,排除已发布ex_code:%s" % (self._taskid,i['ex_code']))
            else:
                try:
                    RunDispatch().insert(_d_rd).execute_rowcount()
                except Exception, e:
                    logging.Alarm("disDailyEx,批次%s,发布失败,可能主键错误,ex_code:%s,%s,sql:%s" % (self._taskid,i['ex_code'],str(e),RunDispatch().insert(_d_rd).echo()))


    def disMonthlyZb(self):
        print 'M111'

    def disMonthlyRule(self):
        print 'M222'

    def disMonthlyEx(self):
        print 'M333'