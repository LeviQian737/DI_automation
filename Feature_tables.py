
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
#desc, abs, count, col, concat, concat_ws, udf, array_construct, countDistinct, when
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import *

from sklearn.model_selection import train_test_split # split  data into training and testing sets
from sklearn.metrics import balanced_accuracy_score, roc_auc_score, make_scorer # for scoring during cross validation
from sklearn.model_selection import GridSearchCV # cross validation
from sklearn.metrics import confusion_matrix # creates a confusion matrix
#from sklearn.m etrics import plot_confusion_matrix # draws a confusion matrix
from sklearn.metrics import mean_squared_error as MSE

from sklearn.metrics import f1_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import accuracy_score

import xgboost as xgb 
#import shap
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import math


# In[2]:


today = datetime.today()
current_month_start = datetime(today.year, today.month, 1)
previous_month_start = datetime(today.year, today.month-1, 1)
#previous_month_end = current_month_start - timedelta(days=1)



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




query1 = """DELETE FROM DATA_LAB_TEST.PREDICTOR.TR_FT_TRAIN WHERE DATE_PREDICT >= ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE), -1)"""  
# DATE_TRUNC('MONTH', CURRENT_DATE)"""
table = session.sql(query1)
table.collect()


start_idx = session.sql("SELECT MAX(IDX) AS START_IDX FROM DATA_LAB_TEST.PREDICTOR.TR_FT_TRAIN").collect()[0]['START_IDX']


query2 = """DELETE FROM DATA_LAB_TEST.PREDICTOR.TR_FT_PRODUCT_LINE WHERE IDX > {0}"""
table = session.sql(query2.format(start_idx))
table.collect()

query3 = """DELETE FROM DATA_LAB_TEST.PREDICTOR.TR_FT_MECHANICS WHERE IDX > {0}"""
table = session.sql(query3.format(start_idx))
table.collect()


all_orders = session.sql("SELECT * FROM TR_ORDERS_DEVICE WHERE PARTYID is not null AND SHOPPARTYID is not null")



orders_DI = all_orders.filter(col("BASE_MODEL").isNotNull())
DI_list = orders_DI.select(col("PARTYID"), col("SHOPPARTYID")).distinct()
orders_DI = orders_DI.join(orders_DI.groupBy('PARTYID').agg(min(col("COMPLETEDATE")).alias("FIRST_DI_PURCHASE")), ['PARTYID'], 'left')


query4 = '''
WITH USERS AS (SELECT PARTYID, FIRST_DI_PURCHASE, DATE_PREDICT, {0} + ROW_NUMBER() OVER(ORDER BY DATE_PREDICT, PARTYID) AS RN
                FROM (SELECT PARTYID, min(COMPLETEDATE) as FIRST_DI_PURCHASE 
                        FROM TR_ORDERS_DEVICE 
                        WHERE BASE_MODEL is not null AND PARTYID is not null AND SHOPPARTYID is not null
                        GROUP BY 1
                    ) A
                JOIN (SELECT DATEADD(MONTH, seq - 1, ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE), -1)) AS DATE_PREDICT
                        FROM (SELECT ROW_NUMBER() OVER(ORDER BY seq4()) AS seq
                                FROM TABLE(GENERATOR(ROWCOUNT => 5 ))
                      ) 
                      WHERE DATE_PREDICT<=ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE), 0)) B
                WHERE DATE_PREDICT > FIRST_DI_PURCHASE )

SELECT PARTYID, BPN, GUID, SHOPPARTYID, SHOPGUID, DATE_PREDICT, FIRST_DI_PURCHASE, YEAR(DATE_PREDICT) AS YEAR_PREDICT, RN AS IDX, 
        COUNTRY, ITEM_SKU, ITEM_PRICE, BASE_MODEL, COMPLETEDATE1 AS COMPLETEDATE, GAP_DAY, TIER, COUNT(CASE WHEN MONTHS_DIFF<=0 THEN 1 END) AS DI_PURCHASED, 
        MAX(LABEL1) AS LABEL1, MAX(LABEL2) AS LABEL2, MAX(LABEL3) AS LABEL3
FROM (
        SELECT U.PARTYID, U.FIRST_DI_PURCHASE, DATE_PREDICT, RN, CEIL(MONTHS_BETWEEN(A.COMPLETEDATE, U.DATE_PREDICT)) AS MONTHS_DIFF,
                CASE WHEN MONTHS_DIFF <= 0 AND DATE(COMPLETEDATE)<U.DATE_PREDICT THEN 0 ELSE 1 END AS FLAG, 
                FIRST_VALUE(BPN) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS BPN,
                FIRST_VALUE(GUID) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS GUID,
                FIRST_VALUE(SHOPPARTYID) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS SHOPPARTYID,
                FIRST_VALUE(SHOPGUID) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS SHOPGUID,
                FIRST_VALUE(COUNTRY) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS COUNTRY,
                FIRST_VALUE(ITEM_SKU) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS ITEM_SKU,
                FIRST_VALUE(ITEM_PRICE) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS ITEM_PRICE,
                FIRST_VALUE(BASE_MODEL) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS BASE_MODEL,
                FIRST_VALUE(TIER) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS TIER,
                FIRST_VALUE(COMPLETEDATE) OVER (PARTITION BY RN ORDER BY FLAG, COMPLETEDATE desc) AS COMPLETEDATE1,
                DATEDIFF('day', COMPLETEDATE1, DATE_PREDICT) AS GAP_DAY,
                CASE WHEN MONTHS_DIFF<=3 AND MONTHS_DIFF>0 THEN 1 ELSE 0 END AS LABEL1, 
                CASE WHEN MONTHS_DIFF<=6 AND MONTHS_DIFF>0 THEN 1 ELSE 0 END AS LABEL2,
                CASE WHEN MONTHS_DIFF<=12 AND MONTHS_DIFF>0 THEN 1 ELSE 0 END AS LABEL3 
        FROM USERS U
        LEFT JOIN (SELECT *, ROW_NUMBER() OVER(PARTITION BY PARTYID ORDER BY COMPLETEDATE) AS RN1
                    FROM TR_ORDERS_DEVICE
                    WHERE BASE_MODEL is not null AND PARTYID is not null AND SHOPPARTYID is not null) A
        ON U.PARTYID = A.PARTYID )
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16'''
di_labels = session.sql(query4.format(start_idx))


di_labels.count()


di_labels.groupBy('LABEL3').count().orderBy('count').show()


df_loyal = session.table('TR_SHOP_LOYALTY')
df_last = di_labels.join(df_loyal.select(col('SHOPPARTYID'), col('LOYALTY')), ['SHOPPARTYID'], 'left')


df_last.filter(col('LOYALTY').isNull()).count()


df_last = df_last.na.fill({"LOYALTY": 0})


df_last = df_last.withColumn('SERIALNUMBER_FUZZY', lit(None))
df_last = df_last.withColumn('LAST_UPDATE_DISTANCE', lit(None))
df_last = df_last.withColumn('UPDATE_COUNT', lit(None))
df_last = df_last.withColumn('UPDATE_FREQUENCY', lit(None))
df_last = df_last.withColumn('HAS_SERIAL', lit(None))


df_last.write.mode("append").save_as_table("TR_FT_TRAIN")


orders_to_predict = all_orders.drop('IDX').join(di_labels.select(col('PARTYID'), col('IDX'), col('DATE_PREDICT')), ['PARTYID'], 'right').withColumn("GAP_DAY", datediff("day", to_date(col('COMPLETEDATE')), to_date(col('DATE_PREDICT')))).filter(col('GAP_DAY') >= 0)
            


orders_2years = orders_to_predict.filter(col('GAP_DAY') < 4 * 90).filter(col('GAP_DAY') >= 1)
orders_2years = orders_2years.withColumn('GAP_DAY', floor(col('GAP_DAY') / 90)).withColumnRenamed("GAP_DAY", 'GAP')   .filter(col("BASE_MODEL").isNull()).filter(col('SHORT').isin(['TS', 'PT', 'HT', 'ES', 'DI'])).groupBy('PARTYID', 'IDX', 'SHORT', 'MAJOR', 'GAP').agg(sum(col('ITEM_PRICE')).alias("PRICE"))
orders_2years = orders_2years.select("*", concat(col('SHORT'), lit('_'), col('MAJOR'), lit('_'), col('GAP')).alias('PRODUCT_LINE')).drop(["SHORT", "MAJOR", "GAP"])


product_lines = []
for i in orders_2years.select(col("PRODUCT_LINE")).distinct().collect():
    product_lines.append(i[0])
    
col_list = ["PARTYID", "IDX"]
col_list.extend(product_lines)

df_line = orders_2years.pivot("PRODUCT_LINE", product_lines).sum("PRICE")

col_map = dict(zip(df_line.columns, col_list))
df_line = df_line.select([col(c).alias(col_map[c]) for c in df_line.columns])
df_line = df_line.na.fill(value=0, subset=product_lines)

col_list = ["PARTYID", "IDX"]
col_list.extend(sorted(product_lines))
df_line = df_line.select(col_list)


df_line_check = session.table('TR_FT_PRODUCT_LINE')
#len(df_line_check.columns), len(df_line.columns)


if all(item in df_line_check.columns for item in df_line.columns):
    if len(df_line_check.columns)==len(df_line.columns):
        df_line.write.mode("append").save_as_table("TR_FT_PRODUCT_LINE")
    else:
        df_line_new = df_line
        new_columns = list(set(df_line_check.columns)-set(df_line.columns))
        for column in new_columns:
            df_line_new = df_line_new.withColumn(column, lit(0))
        df_line_new = df_line_new.select(df_line_check.columns)  
        df_line_new.write.mode("append").save_as_table("TR_FT_PRODUCT_LINE")
else:
    new_columns_to_orig = list(set(df_line.columns)-set(df_line_check.columns))
    for column in new_columns_to_orig:
        df_line_check = df_line_check.withColumn(column, lit(0))
    
    df_line_new = df_line
    new_columns = list(set(df_line_check.columns)-set(df_line.columns))
    for column in new_columns:
        df_line_new = df_line_new.withColumn(column, lit(0))
    df_line_new = df_line_new.select(df_line_check.columns)
    
    df_line_append = df_line_check.union(df_line_new)
    
    tot_columns = ['IDX', 'PARTYID']
    tot_columns.extend(sorted(set(df_line_append.columns)-set(['IDX', 'PARTYID'])))
    df_line_append = df_line_append.select(tot_columns)
    
    df_line_append.write.mode("overwrite").save_as_table("TR_FT_PRODUCT_LINE")
    

orders_to_predict = all_orders.drop('IDX').drop(['BPN', 'GUID', 'SHOPGUID', 'PREDICT_QUARTER']).withColumnRenamed("PARTYID", 'PARTYID_IN_SHOP').join(di_labels.select(col('PARTYID'), col('IDX'), col('DATE_PREDICT'), col('SHOPPARTYID')), ['SHOPPARTYID'], 'right').withColumn("GAP_DAY", datediff("day", to_date(col('COMPLETEDATE')), to_date(col('DATE_PREDICT'))))


df_mechs = orders_to_predict.filter(col('GAP_DAY') < 4 * 90).filter(col('GAP_DAY') >= 1)
df_mechs = df_mechs.withColumn('GAP_DAY', floor(col('GAP_DAY') / 90)).withColumnRenamed("GAP_DAY", 'GAP')                      .select(col('PARTYID'), col('SHOPPARTYID'), col('PARTYID_IN_SHOP'), col('IDX'), col('GAP')).distinct()

df_mechs = df_mechs.groupBy('PARTYID', 'SHOPPARTYID', 'IDX', 'GAP').count().withColumnRenamed("COUNT", "TECH")
df_mechs = df_mechs.pivot('GAP', [0,1,2,3]).sum("TECH")


col_list = ['PARTYID', 'SHOPPARTYID', 'IDX', 'TECH_0', 'TECH_1', 'TECH_2', 'TECH_3']

col_map = dict(zip(df_mechs.columns, col_list))
df_mechs = df_mechs.select([col(c).alias(col_map[c]) for c in df_mechs.columns])
df_mechs = df_mechs.na.fill(value=0, subset=['TECH_0', 'TECH_1', 'TECH_2', 'TECH_3'])


df_mechs.write.mode("append").save_as_table("TR_FT_MECHANICS")



