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

PREDICT_YEAR = 2023

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

query = '''create or replace TABLE DATA_LAB_TEST.PREDICTOR.TR_ORDERS_DEVICE (
    BPN NUMBER(38,0),
    GUID VARCHAR(40),
    ITEM_SKU VARCHAR(40),
    SERIALNUMBER_RAW VARCHAR(16777216),
    SERIALNUMBER VARCHAR(16777216),
    SERIALNUMBER_ADJ VARCHAR(16777216),
    COMPLETEDATE TIMESTAMP_NTZ(9),
    YEAR NUMBER(4,0),
    MONTH NUMBER(2,0),
    PREDICT_QUARTER NUMBER(1,0),
    AGE NUMBER(38,0),
    CALLDAY NUMBER(10,0),
    SHOP_TYPE VARCHAR(100),
    SHOPGUID VARCHAR(16777216),
    SHOPOWNER NUMBER(1,0),
    LINETYPE VARCHAR(16777216),
    SALE_LIST_PRICE FLOAT,
    DISCOUNT_AMOUNT FLOAT,
    SHORT VARCHAR(6),
    MAJOR VARCHAR(6),
    MINOR VARCHAR(6),
    ITEM_PRICE FLOAT,
    BASE_MODEL VARCHAR(30),
    TIER NUMBER(1,0),
    COUNTRY VARCHAR(18),
    FIRST_PURCHASE_YEAR NUMBER(4,0),
    TIME_GAP NUMBER(5,0),
    IDX NUMBER(18,0),
    SERIALNUMBER_FUZZY VARCHAR(16777216),
    PARTYID VARCHAR(18),
    SHOPPARTYID VARCHAR(18)
)
AS (
select 
    A.*, 
    B.FIRST_PURCHASE_YEAR AS FIRST_PURCHASE_YEAR, 
    YEAR(A.completedate)-B.FIRST_PURCHASE_YEAR AS TIME_GAP,
    ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS idx,
    null as SERIALNUMBER_FUZZY,
    ID.PARTYID as PARTYID,
    ID2.PARTYID as SHOPPARTYID
FROM (
    select DISTINCT
    o.client_key AS BPN,
    CASE
        WHEN c.custtypeid = 4 and c.positiontypeid = 157 and c.shopref is not null THEN c.shopref
        ELSE o.custidclientguid
    END GUID,  
    i.item item_sku,
    UPPER(replace(replace(od.serialnumber, ' ', ''), 'USED', '')) as serialnumber_raw,
    CASE 
        WHEN serialnumber_raw LIKE '%-%' AND length(split_part(serialnumber_raw, '-', 1)) NOT IN (3,6,7) 
            
            THEN
            (CASE WHEN (length(split_part(serialnumber_raw, '-', 1)) > length(split_part(serialnumber_raw, '-', 2)))
            THEN UPPER(split_part(serialnumber_raw, '-', 1)) 
            ELSE UPPER(split_part(serialnumber_raw, '-', 2)) END) 
        WHEN serialnumber_raw LIKE '%-%' AND length(split_part(serialnumber_raw, '-', 1)) IN (3,6,7) 
            
            THEN UPPER(LEFT(REGEXP_REPLACE(serialnumber_raw, '([-_+=\.` /;*~])'), 12))
        WHEN serialnumber_raw NOT LIKE '%-%' 
            AND length(serialnumber_raw) BETWEEN 13 AND 14 
            
            THEN UPPER(LEFT(REGEXP_REPLACE(serialnumber_raw, '([-_+=\.` /;*~])'), 12))
        ELSE UPPER(serialnumber_raw)
    END serialnumber_cut, 
    CASE 
        WHEN length(serialnumber_cut) >= 20  
            AND UPPER(LEFT(REGEXP_REPLACE(try_hex_decode_string(serialnumber_cut), '([-\. ])'), 12)) REGEXP '[A-Z0-9]*'
        THEN UPPER(LEFT(REGEXP_REPLACE(try_hex_decode_string(serialnumber_cut), '([-\. ])'), 12))
        WHEN length(serialnumber_cut) = 12 
        THEN serialnumber_cut
    END serialnumber_adj,
    o.completedate,  
    year(o.completedate) as year,
    month(o.completedate) as month,
    CASE WHEN MONTH(o.completedate) <= 3 THEN 1
                        WHEN MONTH(o.completedate) <= 6 THEN 2
                        WHEN MONTH(o.completedate) <= 9 THEN 3
                        WHEN MONTH(o.completedate) <= 12 THEN 4 END AS PREDICT_QUARTER,
    CASE
    WHEN c.custtypeid = 4 THEN
            CASE
            WHEN trunc((to_number(to_char(o.completedate, 'YYYYMMDD'))-to_number(REPLACE(substr(c.birthdate, 0, 10), '-', '')))/ 10000) > 100 THEN 0
            WHEN trunc((to_number(to_char(o.completedate, 'YYYYMMDD'))-to_number(REPLACE(substr(c.birthdate, 0, 10), '-', '')))/ 10000) >= 18 THEN
                     trunc((to_number(to_char(o.completedate, 'YYYYMMDD'))-to_number(REPLACE(substr(c.birthdate, 0, 10), '-', '')))/ 10000)
            ELSE 0
        END
    ELSE -1
    END age,
    c.callday,
    CASE
        WHEN st.type IS NOT NULL THEN st.type
        ELSE 'none'
    END shop_type,
    CASE c.custtypeid
        WHEN 3 THEN o.custidclientguid
        WHEN 4 THEN c.shopref
    END shopguid,
    CASE
            WHEN c.positiontypeid = 157 THEN 1
            WHEN c.custtypeid = 3 THEN 1
            ELSE 0
    END shopowner,
    DECODE(olt.linetypeid, 1, 'SALE', 6, 'TRADEIN') LINETYPE,
    od.listprice sale_list_price,
    od.miscallowance discount_amount,
    substr(i.line, 0, 2) SHORT,
    substr(i.line, 3, 2) MAJOR,
    substr(i.line, 5, 2) MINOR,
    i.curr_item_price item_price,
    d.BASE_MODEL,
    d.tier,
    case when f.company='100' then 'Canada'
        when f.company='130' then 'U.S.'
        when f.company='387' then 'U.K.'
        end as COUNTRY
FROM
    chrome.chromebk.orders o,
    chrome.chromebk.orderdetail od,
    chrome.chromebk.orderlinetype olt,
    data_lab_test.predictor.customer c,
    chrome.chromebk.customer s,
    chrome.chromebk.shoptype st,
    chrome.chromebk.address a,
    chrome.chromesys.recommender_items i,
    data_lab_test.predictor.DI_ITEMS d,
    chrome.chromebk.franchisee_map f
WHERE
    o.client_key = od.client_key
    AND o.clientguid = od.orderidclientguid
    AND od.client_key = olt.client_key
    AND od.clientguid = olt.orderdetailid
    AND o.client_key = c.client_key
    AND o.custidclientguid = c.clientguid
    AND c.client_key = s.client_key
    AND c.shopref = s.clientguid
    AND s.shoptypeid = st.shoptypeid
    AND s.groupid = st.groupid
    AND od.partnumber = i.item
    AND i.item = d.item (+)
    AND c.client_key = a.client_key
    AND c.addrid = a.clientguid
    AND o.client_key = f.client_key (+)
    AND od.isdeleted = 0
    AND olt.isdeleted = 0
    AND olt.linetypeid in (1,6) 
    AND (i.item != 'EEMS343H' 
        OR i.curr_item_price != 1642) 
    AND o.orderstatus = 10
    AND o.ordertypeid = 4
    AND od.quantity > 0
    AND c.isdeleted = 0
    AND s.isdeleted = 0
    AND a.isdeleted = 0
    AND (UPPER(A.NAME1) NOT LIKE '%CASH%'
        OR UPPER(A.NAME2) NOT LIKE '%CASH%'
        OR UPPER(A.NAME1) NOT LIKE 'WALK%'
        OR UPPER(A.NAME2) NOT LIKE 'WALK%')  
    AND o.completedate >= '01-JAN-2013' 
    AND o.isdeleted = 0 ) A
LEFT JOIN 
    (SELECT USER_ID, MIN(YEAR(TO_TIMESTAMP_NTZ(TIMESTAMP/1000))) AS first_purchase_year 
    FROM DATA_LAB_TEST.RECOMMENDER.RECOMMENDER_EXPORT_VIEW2_ALLTIME 
    GROUP BY USER_ID) B 
    ON (A.BPN || '-' || A.GUID) = B.USER_ID 
LEFT JOIN 
    DATA_LAB_TEST.PREDICTOR.UNIVERSAL_ID_VIEW ID
    ON A.GUID = ID.CLIENTGUID AND A.BPN = ID.CLIENTKEY
LEFT JOIN 
    DATA_LAB_TEST.PREDICTOR.UNIVERSAL_ID_VIEW ID2
    ON A.SHOPGUID = ID2.CLIENTGUID AND A.BPN = ID2.CLIENTKEY )'''

table = session.sql(query)
table.collect()