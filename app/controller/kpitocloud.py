# -*- coding: utf-8 -*-
import platform,os
import datetime
from ..models import Old2newCfgWeb,KpiTotalDailyWeb,KpiCompCd005DVerticalWeb
from app import logging
from ..utils import replace_all
import re

class RunMv(object):
    def __init__(self,debugger=False):
        self._pid = '' if platform.system() == 'Windows' else os.getpid()
        self._host = platform.uname()[1]
        self._debugger = debugger

    def mvAll(self):
        _dct_cfg = Old2newCfgWeb().select(flag=0).order('time_id').limit(1).get()
        if not _dct_cfg:
            return False
        _time_id = _dct_cfg['time_id']
        if len(str(_time_id)) == 8:
            _dct = KpiTotalDailyWeb().select(time_id=_time_id,zb_code=_dct_cfg['old_code']).limit(1).query()
            if not _dct:
                logging.Info("日指标%s,批次%s,无数据" % (_dct_cfg['old_code'],_time_id))
                Old2newCfgWeb().update(flag=3).where(time_id=_time_id,old_code=_dct_cfg['old_code'],new_code=_dct_cfg['new_code']).execute()
            else:
                _lock_cnt = Old2newCfgWeb().update(flag=1).where(time_id=_time_id,old_code=_dct_cfg['old_code'],new_code=_dct_cfg['new_code'],flag=0).execute_rowcount()
                if _lock_cnt == 0:
                    pass
                else:
                    logging.Info("日指标(新)%s,批次%s,删除数据" % (_dct_cfg['new_code'],_time_id))
                    KpiCompCd005DVerticalWeb().delete().where(op_time=str(_time_id),kpi_code=_dct_cfg['new_code']).execute()
                    logging.Info("日指标(新)%s,批次%s,插入数据" % (_dct_cfg['new_code'],_time_id))
                    _sql = 'insert into {tab_name} (op_time,dim_code,dim_val,kpi_code,kpi_val,yesterday,last_month,last_year)'.format(tab_name=KpiCompCd005DVerticalWeb().__tablename__) \
                    + ' select time_id,' \
                    + 'case when level=1 then \'PROV_ID\' when level=2 then \'CITY_ID\' when level=3 then \'COUNTY_ID\' else \'MARKET_ID\' end,' \
                    + 'channel_code,\'{kpicode}\',value,yesterday,last_month,last_year'.format(kpicode=_dct_cfg['new_code']) \
                    + ' from {from_tab_name}'.format(from_tab_name=KpiTotalDailyWeb().__tablename__) \
                    + ' where time_id={timeid} and zb_code=\'{zbcode}\' and level in (1,2,3,4,5)'.format(timeid=_time_id,zbcode=_dct_cfg['old_code'])
                    logging.Info("sql:%s" % _sql)
                    KpiCompCd005DVerticalWeb().excutebysql(_sql)
    
