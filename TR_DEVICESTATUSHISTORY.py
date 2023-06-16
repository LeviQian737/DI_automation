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

table = session.sql('''create or replace table DATA_LAB_TEST.PREDICTOR.TR_DEVICESTATUSHISTORY (
    DEVICEID VARCHAR(50),
    PLATFORMTYPE VARCHAR(50),
    DEALERID VARCHAR(50),
    BPNUMBER VARCHAR(50),
    RECORDCREATEDDATE TIMESTAMP_NTZ(9),
    MARKET VARCHAR(10),
    FIRSTTIMEACTIVATEDDATE TIMESTAMP_NTZ(9),
    SUBSCRIBEDSTATUS VARCHAR(50),
    BUNDLENUMBER VARCHAR(50),
    BUNDLEDID VARCHAR(50),
    BUNDLEUPDATEDATE TIMESTAMP_NTZ(9),
    BUNDLEUPDATEDBY VARCHAR(50),
    LICENSERECORDID NUMBER(38,0),
    LASTUPGRADEFREE BOOLEAN,
    SERIALNUMBER_ORIG VARCHAR(50),
    SERIALNUMBER VARCHAR(50)
)
as
( 
select A.DEVICEID,
    max(PLATFORMTYPE) as PLATFORMTYPE,
    max(DEALERID) as DEALERID,
    max(BPNUMBER) as BPNUMBER,
    max(RECORDCREATEDDATE) as RECORDCREATEDDATE,
    MARKET,
    FIRSTTIMEACTIVATEDDATE,
    min(SUBSCRIBEDSTATUS) as SUBSCRIBEDSTATUS,
    A.BUNDLENUMBER,
    max(BUNDLEDID) as BUNDLEDID,
    A.BUNDLEUPDATEDATE,
    max(BUNDLEUPDATEDBY) as BUNDLEUPDATEDBY,
    LICENSERECORDID,
    max(LASTUPGRADEFREE) as LASTUPGRADEFREE,
    MAX(SERIALNUMBER) as SERIALNUMBER_ORIG,
    case when upper(try_hex_decode_string(A.deviceid)) regexp '[A-Z0-9]*'
        and length(upper(try_hex_decode_string(A.deviceid))) = 12
        then upper(try_hex_decode_string(A.deviceid))
    end as SERIALNUMBER
from (select deviceid, left(bundlenumber, 4) as bundlenumber, max(bundleupdatedate) as bundleupdatedate, max(recordid) as recordid
        from diagnostics.snapondevices.devicestatushistory 
        group by deviceid, left(bundlenumber, 4)
) as A
inner join diagnostics.snapondevices.devicestatushistory B
on A.deviceid=B.deviceid AND A.bundlenumber=left(B.bundlenumber, 4) AND A.bundleupdatedate = B.bundleupdatedate 
group by 1, 6, 7, 9, 11, 13, 16
)''')

table.collect()