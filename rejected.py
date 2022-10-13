
# coding: utf-8

# In[ ]:


import pandas as pd

from sqlalchemy import create_engine

pub = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = 172.16.1.18)(PORT = 1521))'
pub += '(CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = sb2prod) ) )'
pub_con = create_engine("oracle+cx_oracle://sbapi:sbapi@" + pub)


def rejected_cons_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-CON-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df


def rejected_comm_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-COM-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df


def rejected_mfcons_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-MFCON-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df


def rejected_mfcomm_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-MFCOMM-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df


def rejected_mgcons_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-MGCON-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df



def rejected_mgcomm_df(dpid):
    df = pd.read_sql(f"""with cys as (
  select
    to_char(add_months(sysdate, -1), 'yyyymm') current_data_cycle,
    to_char(sysdate, 'yyyymm') current_data_position
  from dual
)
    select
        credit_facility_account_number account_number, error_description
    from sbhistory.SB_CONSOLIDATE_ERROR_LOG
    where file_id IN (
        select
            i.file_id
        from ioc_sb_file_info i, cys c, sb_institution_master sim
        where to_char(i.date_reported,'YYYYMM') = current_data_cycle
        and i.institution_id = sim.institution_sequence
        --and i.file_name not like '%-0000%'
        and i.file_name like '%-MGCOMM-%'
        and dpid = '{dpid}'
    )
    AND SEVERITY LIKE '%S%'
    """, pub_con)
    
    return df









