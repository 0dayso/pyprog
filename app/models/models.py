# -*- coding: utf-8 -*-
from ..orm import BaseOrm

class KpiTotalDailyWeb(BaseOrm): __tablename__='pt.kpi_total_daily';__source__='web'
class KpiCompCd005DVerticalWeb(BaseOrm): __tablename__='st.kpi_comp_cd005_d_vertical';__source__='web'
class KpiTotalMonthlyWeb(BaseOrm): __tablename__='pt.kpi_total_monthly';__source__='web'
class KpiCompCd005MVerticalWeb(BaseOrm): __tablename__='st.kpi_comp_cd005_m_vertical';__source__='web'
class Old2newCfgWeb(BaseOrm): __tablename__='nwh.old2new_cfg';__source__='web'