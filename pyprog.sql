create database pt DEFAULT CHARACTER SET utf8;

drop table pt.kpi_total_daily;
create table pt.kpi_total_daily
(
time_id    int,
zb_code    varchar(64),
zb_name    varchar(64),
channel_code    varchar(64),
channel_name    varchar(64),
value      decimal(28,2),
yesterday  decimal(28,2),
last_month decimal(28,2),
last_year  decimal(28,2),
level      int
);

insert into pt.kpi_total_daily
(time_id,zb_code,zb_name,channel_code,value,yesterday,last_month,last_year,level)
values
(20170903,'OLD','jdjd','HB',1,2,3,4,1);


create table st.kpi_comp_cd005_d_vertical
(
op_time    varchar(32),
dim_code   varchar(64),
dim_val    varchar(64),
kpi_code   varchar(64),
kpi_val    decimal(28,2),
yesterday  decimal(28,2),
last_month decimal(28,2),
last_year  decimal(28,2)
);

drop table nwh.old2new_cfg;
create table nwh.old2new_cfg
(
old_code   varchar(32),
new_code   varchar(32),
time_id    int,
flag       int
);

insert into nwh.old2new_cfg
values
('OLD','NEW',20170903,0);

insert into st.kpi_comp_cd005_d_vertical (op_time,dim_code,dim_val,kpi_code,kpi_val,yesterday,last_month,last_year) select time_id,case when level=1 then 'PROV_ID' when level=2 then 'CITY_ID' when level=3 then 'COUNTY_ID' else 'MARKET_ID' end,channel_code,'NEW',value,yesterday,last_month,last_year from pt.kpi_total_daily where time_id=20170903 and zb_code='OLD' and level in (1,2,3,4,5)