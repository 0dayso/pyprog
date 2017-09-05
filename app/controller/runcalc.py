# -*- coding: utf-8 -*-
import platform,os
import datetime
from ..models import ZbDef,RunDispatch,ErrMsgLog,BoiMidDaily,BoiMidMonthly,BoiTotalDaily,BoiTotalMonthly
from app import logging
from .comm import chkDepend,chkDependLastDay,replace_taskid
from ..utils import replace_all
import re

class Runcalc(object):
    def __init__(self,debugger=False):
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]
        self._debugger = debugger

    def calcAll(self,id):
        _dct_RunDispatch = RunDispatch().select(id=id).get()
        if len(_dct_RunDispatch['etl_cycle_id']) == 8:
            if _dct_RunDispatch['type'] == 'zb':
                self.calcDailyZb(id=id,taskid=_dct_RunDispatch['etl_cycle_id'],gbascode=_dct_RunDispatch['gbas_code'])
        elif len(_dct_RunDispatch['etl_cycle_id']) == 6:
            if _dct_RunDispatch['type'] == 'zb':
                self.calcMonthlyZb(id=id,taskid=_dct_RunDispatch['etl_cycle_id'],gbascode=_dct_RunDispatch['gbas_code'])
        else:
            raise Exception('批次值错误,etl_cycle_id='+_dct_RunDispatch['etl_cycle_id'])

    def calcErrDel(self,id,errmsg):
        _dct_RunDispatch = RunDispatch().select(id=id).get()
        _dct = {
                'etl_status'    : '-'+_dct_RunDispatch['etl_status'] if _dct_RunDispatch['etl_status'][:1] != '-' else _dct_RunDispatch['etl_status'],
                'exec_end_time' : str(datetime.datetime.now()),
                'err_msg'       : errmsg,
                'err_time'      : str(datetime.datetime.now())
                }
        RunDispatch().update(_dct).where(id=id).execute()
        _dct_RunDispatch = RunDispatch().select(id=id).get()
        _dct_ErrMsgLog = {
                        'type'      : _dct_RunDispatch['type'],
                        'type_id'   : _dct_RunDispatch['id'],
                        'gbas_code' : _dct_RunDispatch['gbas_code'],
                        'err_msg'   : _dct_RunDispatch['err_msg'],
                        'err_time'  : _dct_RunDispatch['err_time']
                        }
        ErrMsgLog().insert(_dct_ErrMsgLog).execute()

    def getCalcTask(self):
        _dct = {
                'etl_status'      : '3',
                'pid'             : str(self._pid),
                'hostname'        : self._host,
                'exec_start_time' : str(datetime.datetime.now())
                }
        _inst_RunDispatch = RunDispatch().select(type='zb').in_('etl_status',['0','1']).order('etl_cycle_id,priority,depend_type desc').limit(100)
        logging.Debug("runcalc,获取已发布列表sql:%s" % (_inst_RunDispatch.echo()))
        _dct_RunDispatch = _inst_RunDispatch.query()
        for i in _dct_RunDispatch:
            logging._taskname = i['type']+'['+i['gbas_code']+']'
            #etl_status='1'的任务强制执行或依赖已满足
            if i['etl_status'] == '1':
                logging.Debug("runcalc,id:%s,指标:%s,批次:%s,不判断依赖强制执行" % (i['id'],i['gbas_code'],i['etl_cycle_id']))
                _cnt = RunDispatch().update(_dct).where(id=i['id'],etl_status='1').execute_rowcount()
                if _cnt == 1:
                    return i['id']
                else:
                    pass
            #depend_type='2'的任务判断昨日上一批次指标依赖满足,同时当前批次依赖满足时执行
            elif i['depend_type'] == '2':
                if chkDependLastDay(taskid=i['etl_cycle_id'],gbascode=i['gbas_code']) and chkDepend(taskid=i['etl_cycle_id'],procdepend=i['proc_depend'],gbasdepend=i['gbas_depend']):
                    _cnt = RunDispatch().update(_dct).where(id=i['id'],etl_status='0').execute_rowcount()
                    if _cnt == 1:
                        return i['id']
                    else:
                        pass
            #depend_type='1'的任务判断依赖满足时执行
            else:
                if chkDepend(taskid=i['etl_cycle_id'],procdepend=i['proc_depend'],gbasdepend=i['gbas_depend']):
                    _cnt = RunDispatch().update(_dct).where(id=i['id'],etl_status='0').execute_rowcount()
                    if _cnt == 1:
                        return i['id']
                    else:
                        pass
        return 0

    def calcDailyZb(self,id,taskid,gbascode):
        _dct_ZbDef = ZbDef().select(zb_code=gbascode).get()
        #使用zb_def的sql计算指标gbas_val,gbas_val1
        _inst_del_BoiMidDaily = BoiMidDaily().delete().where(id=id)
        logging.Info("calcDailyZb,清除数据sql:%s" % _inst_del_BoiMidDaily.echo())
        _inst_del_BoiMidDaily.execute()
        _sql = 'insert into {tab_name} (id, time_id, gbas_code, gbas_val, gbas_val1)'.format(tab_name=BoiMidDaily().__tablename__) \
             + ' select {id},{taskid},\'{gbascode}\',tmp_tab.* from ('.format(id=id,taskid=taskid,gbascode=gbascode) \
             + replace_taskid(_dct_ZbDef['zb_def'],taskid) \
             + ') tmp_tab'
        logging.Info("calcDailyZb,准备执行sql:%s" % _sql)
        try:
            BoiMidDaily().insertbysql(_sql)
        except Exception, e:
            raise
        #使用rule_def的sql计算规则结果rule_val
        _dct_BoiMidDaily = BoiMidDaily().select(id=id).query()
        if len(_dct_BoiMidDaily) == 1:
            pass
        else:
            raise Exception('calcDailyZb,指标sql配置错误,要求结果:仅一行纪录,两个字段;sql:'+_sql)
        _d = {
            'A' : 0 if _dct_BoiMidDaily[0]['gbas_val']==None else _dct_BoiMidDaily[0]['gbas_val'],
            'B' : 0 if _dct_BoiMidDaily[0]['gbas_val1']==None else _dct_BoiMidDaily[0]['gbas_val1']
            }
        _formula = _dct_ZbDef['rule_def'].format(**_d)
        logging.Info("calcDailyZb,准备执行公式:%s,%s" % (_dct_ZbDef['rule_def'],_formula))
        try:
            if '/{B}' in replace_all(_dct_ZbDef['rule_def']) and _d['B'] == 0:
                _rule_val=100
            elif '/{A}' in replace_all(_dct_ZbDef['rule_def']) and _d['A'] == 0:
                _rule_val=100
            else:
                _rule_val = eval(_formula)
        except Exception, e:
            raise Exception('calcDailyZb,公式执行错误:'+_formula)
        #使用rule_val计算是否稽核通过
        _comp_str = str(_rule_val)+_dct_ZbDef['comp_oper']+str(_dct_ZbDef['comp_val'])
        try:
            _comp_re = eval(_comp_str)
        except Exception, e:
            raise Exception('calcDailyZb,公式执行错误:'+_comp_str)
        _audit_status = '1' if _comp_re else '0'
        _audit_remark = '通过' if _comp_re else '不通过'
        #生成最终指标规则结果到BoiTotalDaily
        _inst_del_BoiTotalDaily = BoiTotalDaily().delete().where(time_id=int(taskid),gbas_code=gbascode)
        logging.Info("calcDailyZb,清除数据sql:%s" % _inst_del_BoiTotalDaily.echo())
        _inst_del_BoiTotalDaily.execute()
        _dct_BoiTotalDaily = {
                'time_id'      : int(taskid),
                'gbas_code'    : gbascode,
                'gbas_name'    : _dct_ZbDef['zb_name'],
                'gbas_val'     : _dct_BoiMidDaily[0]['gbas_val'],
                'gbas_val1'    : _dct_BoiMidDaily[0]['gbas_val1'],
                'rule_val'     : _rule_val,
                'rule_type'    : _dct_ZbDef['rule_type'],
                'rule_def'     : _dct_ZbDef['rule_def'],
                'comp_oper'    : _dct_ZbDef['comp_oper'],
                'comp_val'     : _dct_ZbDef['comp_val'],
                'audit_status' : _audit_status,
                'audit_remark' : _audit_remark,
                'create_time'  : str(datetime.datetime.now())
                }
        _inst_BoiTotalDaily = BoiTotalDaily().insert(_dct_BoiTotalDaily)
        logging.Info("calcDailyZb,插入数据sql:%s" % _inst_BoiTotalDaily.echo())
        _inst_BoiTotalDaily.execute()
        #更新etl_status='4'
        _dct_rd_zb_over = {
                'etl_status'    : '4',
                'exec_end_time' : str(datetime.datetime.now())
                }
        RunDispatch().update(_dct_rd_zb_over).where(id=id).execute()
        #判断是否告警
        _audit_msg = '规则:'+gbascode+':'\
                    +_dct_BoiTotalDaily['rule_def']+_dct_BoiTotalDaily['comp_oper']+str(_dct_BoiTotalDaily['comp_val'])+';'\
                    +_comp_str+';'\
                    +_audit_remark
        if _dct_ZbDef['rule_type'] == '1' and not _comp_re:
            _dct_audit = {
                        'etl_status'  : '5',
                        'audit_msg'   : '强规则稽核不通过,需要告警;'+_audit_msg
                        }
            logging.Info("calcDailyZb,批次:%s,强规则稽核不通过,%s" % (taskid,_audit_msg))
        elif _dct_ZbDef['rule_type'] == '1':
            _dct_audit = {
                        'etl_status'  : '6',
                        'audit_msg'   : '强规则稽核通过,不告警;'+_audit_msg
                        }
            logging.Info("calcDailyZb,批次:%s,强规则稽核通过,不告警,%s" % (taskid,_audit_msg))
        else:
            _dct_audit = {
                        'etl_status'  : '6',
                        'audit_msg'   : '弱规则不告警;'+_audit_msg
                        }
            logging.Info("calcDailyZb,批次:%s,弱规则不告警,%s" % (taskid,_audit_msg))
        RunDispatch().update(_dct_audit).where(id=id).execute()

    # def calcDailyRule(self,taskid,gbascode):
    #     _dct_RuleDef = RuleDef().select(rule_code=gbascode).get()
    #     _formula = replace_all(_dct_RuleDef['rule_def'])
    #     logging.Info("calcDailyRule,准备执行公式:%s" % _formula)
    #     _p = re.compile('[\+\-\*\/\(\)]')
    #     _lst = _p.split(_formula)
    #     _dct = {}
    #     for i in sorted(_lst, key=len, reverse=True):
    #         if i == '':
    #             pass
    #         else:
    #             _formula=_formula.replace(i,'{'+i+'}')
    #             _inst_BoiTotalDaily=BoiTotalDaily().select(time_id=int(taskid),gbas_code=i)
    #             _dct_BoiTotalDaily=_inst_BoiTotalDaily.query()
    #             if len(_dct_BoiTotalDaily)==1:
    #                 _dct[_dct_BoiTotalDaily[0]['gbas_code']] = _dct_BoiTotalDaily[0]['gbas_val']
    #             else:
    #                 raise Exception('规则包含的指标:%s,批次:%s,包含多个或0个值;sql:%s' % (gbascode,taskid,_inst_BoiTotalDaily.echo()))
    #     _val=eval(_formula.format(**_dct))
    #     _inst_del_BoiTotalDaily = BoiTotalDaily().delete().where(time_id=int(taskid),gbas_code=gbascode)
    #     logging.Info("calcDailyRule,清除数据sql:%s" % _inst_del_BoiTotalDaily.echo())
    #     _inst_del_BoiTotalDaily.execute()
    #     _dct_BoiRuleDaily = {
    #                         'time_id'   : int(taskid),
    #                         'gbas_code' : gbascode,
    #                         'gbas_name' : _dct_RuleDef['rule_name'],
    #                         'gbas_val'  : _val
    #                         }
    #     _inst_BoiRuleDaily = BoiTotalDaily().insert(_dct_BoiRuleDaily)
    #     logging.Info("calcDailyRule,插入数据sql:%s" % _inst_BoiRuleDaily.echo())
    #     _inst_BoiRuleDaily.execute()
        



