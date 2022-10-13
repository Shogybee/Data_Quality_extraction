
# coding: utf-8

# In[ ]:

import pandas as pd

from sqlalchemy import create_engine

pub = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.18)(PORT = 1521)) '
pub += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = sb2prod) ) )'
pub_con = create_engine("oracle+cx_oracle://azeezayinla:azeezayinla@" + pub)

def mf_comm_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Corporate' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.acc_status account_status,
  fac.current_bal balance,
  fac.asset_classification,
  fac.over_due_amt_int amount_overdue,
  max_num_days_due days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_mfcomm_account_lookup fac,
  sbinternal.sb_mfcomm_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.commercial_master cm,
  rpt_cy r
where
  fac.current_bal > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)

    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df

def mf_cons_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Consumer' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.acc_status account_status,
  fac.current_bal balance,
  fac.asset_classification,
  fac.over_due_amt_pricipal amount_overdue,
  max_num_days_due days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_mfcons_account_lookup fac,
  sbinternal.sb_mfcons_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.consumer_master cm,
  rpt_cy r
where
  fac.current_bal > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)
    
    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df
    
def comm_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Corporate' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.acc_status account_status,
  fac.current_bal balance,
  fac.asset_classification,
  fac.over_due_amt_pricipal amount_overdue,
  max_num_days_due days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_comm_account_lookup fac,
  sbinternal.sb_comm_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.commercial_master cm,
  rpt_cy r
where
  fac.current_bal > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)

    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df

def cons_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Consumer' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.acc_status account_status,
  fac.current_bal balance,
  fac.asset_classification,
  fac.over_due_amt_pricipal amount_overdue,
  max_num_days_due days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_cons_account_lookup fac,
  sbinternal.sb_cons_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.consumer_master cm,
  rpt_cy r
where
  fac.current_bal > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)
    
    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df
    
def mg_cons_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Consumer' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.account_status account_status,
  fac.account_balance balance,
  fac.asset_classification,
  fac.amt_overdue_principal amount_overdue,
  days_overdue_principal days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_mgcons_account_lookup fac,
  sbinternal.sb_mgcons_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.consumer_master cm,
  rpt_cy r
where
  fac.account_balance > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)
    
    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df
    
def mg_comm_df(dpid):
    df = pd.read_sql(f"""with rpt_cy as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
select
  distinct to_char(sysdate, 'dd-mon-yyyy') date_extracted,
  current_data_cycle,
  'Corporate' Type,
  fac.data_prdr_id,
  cm.name,
  sim.institution_name,
  fac.structure_id account_number,
  fac.currency,
  fac.account_status account_status,
  fac.account_balance balance,
  fac.asset_classification,
  fac.amt_overdue_principal amount_overdue,
  days_overdue_principal days_overdue,
  to_char(fac.date_reported, 'dd-mon-yyyy') last_date_reported
from sbinternal.sb_mgcomm_account_lookup fac,
  sbinternal.sb_mgcomm_subject_lookup sub,
  sbsystem.sb_institution_master sim,
  sbinternal.commercial_master cm,
  rpt_cy r
where
  fac.account_balance > 0
  and sub.isdelinked is null
  and to_char(fac.date_reported, 'YYYYMM') < current_data_cycle
  and sub.ruid = cm.ruid
  and fac.unique_root_id = sub.unique_root_id
  and fac.data_prdr_id = sim.institution_id
  and fac.data_prdr_id = sub.data_prdr_id
  and fac.data_prdr_id IN ('{dpid}')""", pub_con)

    df['name'] = df.name.apply(lambda x: x.replace('{"EN":"', '').replace('"}', ''))
    df.columns = df.columns.str.upper()

    return df

