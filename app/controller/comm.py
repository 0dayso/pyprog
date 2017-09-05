# -*- coding: utf-8 -*-
import datetime
from ..models import DpEtlCom,RunDispatch,ZbDef,ExDef,GlobalVal
from ..utils import replace_all
from app import logging
from ..config import ORM_TYPE

def chk_taskid(taskid):
    try:
        if len(taskid) == 6: datetime.datetime.strptime(taskid,'%Y%m')
        elif len(taskid) == 8: datetime.datetime.strptime(taskid,'%Y%m%d')
    except ValueError, ex:
        print "时间参数值%s,批次异常" % taskid
        exit(1)

def chkDependLastDay(taskid,gbascode):
    """taskid:20170726
       _taskid_datetime:2017-07-26 00:00:00
       _condi_date:2017-07-26"""
    _taskid_datetime = datetime.datetime.strptime(taskid,'%Y%m%d')
    _last_taskid_datetime = _taskid_datetime + datetime.timedelta(days=-1)
    _last_taskid = datetime.date.strftime(_last_taskid_datetime,'%Y%m%d')
    _gbascode = replace_all(gbascode)
    if RunDispatch().select(gbas_code=_gbascode,etl_cycle_id=str(_last_taskid)).in_('etl_status',['6']).query():
        logging.Debug("chkDependLastDay,gbas:%s,批次:%s,依赖满足" % (_gbascode,_last_taskid))
        return True
    else:
        logging.Debug("chkDependLastDay,gbas:%s,批次:%s,依赖不满足" % (_gbascode,_last_taskid))
        return False

def chkDepend(taskid,procdepend='',gbasdepend=''):
    _procdepend = '' if procdepend==None else replace_all(procdepend)
    _gbasdepend = '' if gbasdepend==None else replace_all(gbasdepend)
    if _procdepend == '' and _gbasdepend == '':
        return True
    if _procdepend != '':
        _arr_procdepend = _procdepend.split(';')
        for _pd in _arr_procdepend:
            if _pd == '':
                pass
            elif DpEtlCom().select(etl_state=3,etl_progname=_pd).where2('etl_cycle_id>'+str(taskid)).query():
                logging.Debug("chkDepend,程序:%s,批次:%s,依赖满足" % (_pd,taskid))
            else:
                logging.Debug("chkDepend,程序:%s,批次:%s,依赖不满足" % (_pd,taskid))
                return False
    if _gbasdepend != '':
        _arr_gbasdepend = _gbasdepend.split(';')
        for _gd in _arr_gbasdepend:
            if _gd == '':
                pass
            elif RunDispatch().select(gbas_code=_gd,etl_cycle_id=str(taskid)).in_('etl_status',['6']).query():
                logging.Debug("chkDepend,gbas:%s,批次:%s,依赖满足" % (_gd,taskid))
            else:
                logging.Debug("chkDepend,gbas:%s,批次:%s,依赖不满足" % (_gd,taskid))
                return False
    return True

def chkDependOfExp(taskid,gbascode):
    """taskid:20170726
       _taskid_datetime:2017-07-26 00:00:00
       _condi_date:2017-07-26"""
    _taskid_datetime = datetime.datetime.strptime(taskid,'%Y%m%d')
    _condi_date=datetime.date.isoformat(_taskid_datetime)
    _dct_ExDef = ExDef().select(ex_code=gbascode).get()
    if not _dct_ExDef:
        logging.Debug("runexp,接口:%s,在ex_def中未找到配置信息" % gbascode)
        return False
    _inst_rule_depend = ZbDef().select(cycle='daily',status='1').where2('boi_code like \'%%'+_dct_ExDef['boi_code']+'%%\' and online_date<=\''+_condi_date+'\' and offline_date>=\''+_condi_date+'\'')
    logging.Debug("runexp,接口:%s,批次:%s,查找依赖的sql:%s" % (gbascode,taskid,_inst_rule_depend.echo()))
    _dct_rule_depend = _inst_rule_depend.query()
    for _gd in _dct_rule_depend:
        if RunDispatch().select(gbas_code=_gd['zb_code'],etl_cycle_id=str(taskid)).in_('etl_status',['6']).query():
            logging.Debug("chkDependOfExp,规则:%s,批次:%s,依赖满足" % (_gd['zb_code'],taskid))
        else:
            logging.Debug("chkDependOfExp,规则:%s,批次:%s,依赖不满足" % (_gd['zb_code'],taskid))
            return False
    return True

def replace_taskid(sql,taskid):
    if ORM_TYPE=='mysql':
        _sql=sql.replace('&TASK_ID',taskid).replace('&MTASK_ID',str(taskid)[:6])
        return _sql
    else:
        _sql=sql
        _dct_GlobalVal = GlobalVal().select(var_type='SQL').query()
        for _gv in _dct_GlobalVal:
            _gv_sql = _gv['sql_def'].replace('&TASK_ID',taskid)
            try:
                _gv_val = GlobalVal().getbysql(_gv_sql).values()[0]
                _sql = _sql.replace(_gv['var_name'],_gv_val)
            except Exception, e:
                pass
        return _sql
        