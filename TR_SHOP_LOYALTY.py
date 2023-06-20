import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
#desc, abs, count, col, concat, concat_ws, udf, array_construct, countDistinct, when
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import *

import numpy as np
np.float = float

import pandas as pd
from datetime import datetime

PREDICT_YEAR = datetime.today().year

SCHEME = "https"
ACCOUNT = "wa15423.us-east-2.aws"
HOST = "wa15423.us-east-2.aws.snowflakecomputing.com"
PORT = "443"
WAREHOUSE = "ANALYTICS_S"
ROLE = "SVC_DATALAB_ROLE"
DATABASE = "DATA_LAB_TEST"
SCHEMA = "PREDICTOR"

connection_parameters = {
        "account": ACCOUNT,
        "user": 'svc_datalab',
        "password": 'p4Xcwd3JmZz;6j!gs.vtg',
        "role": ROLE,
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        "schema": SCHEMA
      }

session = Session.builder.configs(connection_parameters).create()


query = '''Insert into DATA_LAB_TEST.PREDICTOR.TR_SHOP_LOYALTY as
with p as
(select o.bpn, 
    o.shopguid, 
    count(distinct o.serialnumber_fuzzy) as DI_CNT,
    count(distinct o.guid) as TECH_CNT
from DATA_LAB_TEST.PREDICTOR.TR_ORDERS_DEVICE o,
    DATA_LAB_TEST.PREDICTOR.TR_TTEMPOPPLISTDATA t,
    (select distinct bpn, guid 
    from DATA_LAB_TEST.PREDICTOR.TR_ORDERS_DEVICE 
    where year(completedate) >= '{0}' and year(completedate) < '{1}'
        and callday < 6 
        and callday > 0) c 
where o.short = 'DI'
    and o.major in ('PF', 'SF')
    and year(t.upgradedate) >= '{0}'
    and o.serialnumber_fuzzy = t.serialnumber
    and o.bpn = c.bpn 
    and o.guid = c.guid 
group by o.bpn, o.shopguid
order by TECH_CNT, DI_CNT
)
select 
    bpn, 
    shopguid, 
    DI_CNT,
    TECH_CNT,
    ROW_NUMBER() OVER (
      PARTITION BY TECH_CNT
      ORDER BY DI_CNT
   ) RN,
    count(*) OVER (
      PARTITION BY TECH_CNT
   ) TOTAL_SHOPS,
    case when RN / TOTAL_SHOPS < 0.8 then 0 else 1 end
    loyalty
from p
order by TECH_CNT desc, RN asc'''

table = session.sql('''Truncate table DATA_LAB_TEST.PREDICTOR.TR_SHOP_LOYALTY''')
table.collect()

table = session.sql(query.format(PREDICT_YEAR-1, PREDICT_YEAR))
table.collect()