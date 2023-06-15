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

table = session.sql('''create or replace table DATA_LAB_TEST.PREDICTOR.TR_TTEMPOPPLISTDATA AS
(select *, 
    case when upper(platform) like '%ZEUS%' then 'ZEUS'
        when upper(platform) like '%VERUS%' then 'VERUS'
        when upper(platform) like '%VERDICT%' then 'VERDICT'
        when upper(platform) like '%TRITON%' then 'TRITON'
        when upper(platform) like '%MODIS%' then 'MODIS'
        when upper(platform) like '%APOLLO%' then 'APOLLO'
        when upper(platform) like '%SOLUS%' then 'SOLUS'
        when upper(platform) like '%ETHOS%' then 'ETHOS'
        when upper(platform) like '%MICROSCAN%' then 'MICROSCAN'
        when upper(platform) like '%VANTAGE%' then 'VANTAGE'
        when upper(platform) like '%P1000%' then 'P1000'
        when upper(platform) = 'EEMS348' then 'ZEUS'
        else 'SOLUS' -- just zbout 200 serial number that is unknown or empty, so assign it to the most popular base model
    end BASE_MODEL, 
    DECODE(BASE_MODEL, 'APOLLO', 3, 'SOLUS',
        3, 'ETHOS', 4, 'MICROSCAN', 4, 'VANTAGE', 4, 'P1000', 4, 'MODIS', 2, 'TRITON',
        2, 'VERDICT', 1, 'VERUS', 1, 'ZEUS', 1, '000', '0') TIER,
    UPPER(try_hex_decode_string(cartridgeelectronicserialnumber)) as serialnumber_tmp,
    case when serialnumber_tmp REGEXP '[A-Z0-9]*'
        and length(serialnumber_tmp) = 12
        then serialnumber_tmp
    else null
    end as serialnumber,
    replace(customerphone, '-', '') as phonenumber_adj
from DIAGNOSTICS.BUSINESS_DATA.TTEMPOPPLISTDATA
where FRANCHISEECOUNTRYCODE = 'US')''')

table.collect()