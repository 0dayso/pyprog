# -*- coding: utf-8 -*-
import platform,os
import datetime
from ..models import ZbDef,ExDef,RunDispatch,ErrMsgLog
from app import logging
from .comm import chkDepend,chkDependLastDay,chkDependOfExp,replace_taskid
from ..utils import replace_all


class Runexp(object):
    def __init__(self,debugger=False):
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]
        self._debugger = debugger

    def expAll(self,id):
        _dct_RunDispatch = RunDispatch().select(id=id).get()
        if len(_dct_RunDispatch['etl_cycle_id']) == 8:
            self.expDaily(id=id,taskid=_dct_RunDispatch['etl_cycle_id'],gbascode=_dct_RunDispatch['gbas_code'])
        elif len(_dct_RunDispatch['etl_cycle_id']) == 6:
            self.expMonthly(id=id,taskid=_dct_RunDispatch['etl_cycle_id'],gbascode=_dct_RunDispatch['gbas_code'])
        else:
            raise Exception('批次值错误,etl_cycle_id='+_dct_RunDispatch['etl_cycle_id'])
        # _dct = {
        #         'etl_status'    : '2',
        #         'exec_end_time' : str(datetime.datetime.now())
        #         }
        # RunDispatch().update(_dct).where(id=id).execute()

    def expErrDel(self,id,errmsg):
        _dct_RunDispatch = RunDispatch().select(id=int(id)).get()
        _dct = {
                'etl_status'    : '-'+_dct_RunDispatch['etl_status'] if _dct_RunDispatch['etl_status'][:1] != '-' else _dct_RunDispatch['etl_status'],
                'exec_end_time' : str(datetime.datetime.now()),
                'err_msg'       : errmsg,
                'err_time'      : str(datetime.datetime.now())
                }
        RunDispatch().update(_dct).where(id=int(id)).execute()
        _dct_ErrMsgLog = {
                        'type'      : _dct_RunDispatch['type'],
                        'type_id'   : _dct_RunDispatch['id'],
                        'gbas_code' : _dct_RunDispatch['gbas_code'],
                        'err_msg'   : _dct_RunDispatch['err_msg'],
                        'err_time'  : _dct_RunDispatch['err_time']
                        }
        ErrMsgLog().insert(_dct_ErrMsgLog).execute()

    def getExpTask(self):
        _dct = {
                'etl_status'      : '3',
                'pid'             : str(self._pid),
                'hostname'        : self._host,
                'exec_start_time' : str(datetime.datetime.now())
                }
        _inst_RunDispatch = RunDispatch().select(type='exp').in_('etl_status',['0','1','2']).order('etl_cycle_id,priority,depend_type desc').limit(100)
        logging.Debug("runexp,获取已发布列表sql:%s" % (_inst_RunDispatch.echo()))
        _dct_RunDispatch = _inst_RunDispatch.query()
        for i in _dct_RunDispatch:
            logging._taskname = i['type']+'['+i['gbas_code']+']'
            #etl_status='1'的任务强制执行
            if i['etl_status'] == '1':
                logging.Debug("runexp,id:%s,接口:%s,批次:%s,不判断依赖执行" % (i['id'],i['gbas_code'],i['etl_cycle_id']))
                _cnt = RunDispatch().update(_dct).where(id=i['id'],etl_status='1').execute_rowcount()
                if _cnt == 1:
                    return i['id']
                else:
                    pass
            else:
                pass
            #检查真实依赖(接口相关规则是否执行成功)是否满足
            _chk_real_depend_status = chkDependOfExp(i['etl_cycle_id'],i['gbas_code'])
            #检查接口前一批次是否满足
            _chk_lastday_status = chkDependLastDay(taskid=i['etl_cycle_id'],gbascode=i['gbas_code'])
            #检查数据依赖(接口配置表ex_def中proc_depend,gbas_depend字段是否满足)是否满足
            _chk_data_depend_status = chkDepend(taskid=i['etl_cycle_id'],procdepend=i['proc_depend'],gbasdepend=i['gbas_depend'])
            logging.Debug("runexp,id:%s,接口:%s,批次:%s,真实依赖,前一批次依赖,数据依赖判断结果%s,%s,%s" % (i['id'],i['gbas_code'],i['etl_cycle_id'],_chk_real_depend_status,_chk_lastday_status,_chk_data_depend_status))
            if _chk_real_depend_status:
                #depend_type='2'的任务判断昨日上一批次指标依赖满足,同时当前批次依赖满足时执行
                if i['depend_type'] == '2':
                    if _chk_lastday_status:
                        _cnt = RunDispatch().update(_dct).where(id=i['id']).in_('etl_status',['0','2']).execute_rowcount()
                        if _cnt == 1:
                            return i['id']
                        else:
                            pass
                #depend_type='1'的任务判断依赖满足时执行
                else:
                    _cnt = RunDispatch().update(_dct).where(id=i['id']).in_('etl_status',['0','2']).execute_rowcount()
                    if _cnt == 1:
                        return i['id']
                    else:
                        pass
            #检查数据依赖是否满足
            if i['etl_status'] == '0' and _chk_data_depend_status:
                RunDispatch().update(etl_status='2').where(id=i['id'],etl_status='0').execute_rowcount()
        return 0

    def expDaily(self,id,taskid,gbascode):
        print id,taskid,gbascode,'执行导出-----------'
