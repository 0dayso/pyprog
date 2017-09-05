# -*- coding: utf-8 -*-
from ..orm import BaseOrm

class ZbDef(BaseOrm): __tablename__='gbas.zb_def'
class ExDef(BaseOrm): __tablename__='gbas.ex_def'
class RunDispatch(BaseOrm): __tablename__='gbas.run_dispatch'
class ErrMsgLog(BaseOrm): __tablename__='gbas.err_msg_log'
class DpEtlCom(BaseOrm): __tablename__='gbas.dp_etl_com' #nwh
class BoiMidDaily(BaseOrm): __tablename__='gbas.boi_mid_daily'
class BoiMidMonthly(BaseOrm): __tablename__='gbas.boi_mid_monthly'
class BoiTotalDaily(BaseOrm): __tablename__='gbas.boi_total_daily'
class BoiTotalMonthly(BaseOrm): __tablename__='gbas.boi_total_monthly'
class TaskConfig(BaseOrm): __tablename__='gbas.task_config' #boi
class ProcAlarm(BaseOrm): __tablename__='gbas.proc_alarm' #nwh
class MonitorCfg(BaseOrm): __tablename__='gbas.monitor_cfg'
class MonitorLog(BaseOrm): __tablename__='gbas.monitor_log'


class GlobalVal(BaseOrm): __tablename__='nwh.global_val'