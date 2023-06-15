
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import *
#desc, abs, count, col, concat, concat_ws, udf, array_construct, countDistinct, when
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import *

import pandas as pd
import numpy as np
from datetime import datetime
import math



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


all_orders = session.sql("SELECT * FROM TR_ORDERS_DEVICE")


orders_DI = all_orders.filter(col("BASE_MODEL").isNotNull())
DI_list = orders_DI.select(col("PARTYID"), col("SHOPPARTYID")).distinct()
orders_DI = orders_DI.join(orders_DI.groupBy('PARTYID').agg(min(col("COMPLETEDATE")).alias("FIRST_DI_PURCHASE")), ['PARTYID'], 'left')



w_asc = Window.partitionBy('PARTYID').orderBy(col('COMPLETEDATE').asc())

df_DI_upgrades = orders_DI.withColumn("RN", row_number().over(w_asc)).filter(col("rn") > 1).filter(col("YEAR") >= 2014)

df_DI_upgrades = df_DI_upgrades.select(col('BPN'), col('GUID'), col('PARTYID'), col('SHOPPARTYID'), col('SHOPGUID'), col('YEAR'), col('RN')).withColumn('DATE_PREDICT', date_from_parts(col('YEAR'), 1, 1)).withColumn('PREDICT_QUARTER', ((month(col('DATE_PREDICT'))+2)/3).cast(LongType())).withColumn("YEAR_PREDICT", col('YEAR')).withColumn('RN', col('RN')).withColumn('LABEL', lit(1)).drop('YEAR')


orders_DI_cust = all_orders.join(orders_DI.select(col("PARTYID"), col('FIRST_DI_PURCHASE')).distinct(), ['PARTYID'], 'inner').filter(col('COMPLETEDATE') > col('FIRST_DI_PURCHASE') )

for year in range(2014, 2023): 
    orders_ngt_cur = orders_DI_cust.filter((col('YEAR') == year))\
    .groupBy('PARTYID','YEAR').agg(sum(col("BASE_MODEL").isNotNull().cast('integer')).alias("CNT_DI"))\
    .filter(col('CNT_DI') == 0).drop('CNT_DI')\
    .withColumn('DATE_PREDICT', date_from_parts(year, 1, 1))\
    .withColumn('PREDICT_QUARTER', ((month(col('DATE_PREDICT'))+2)/3).cast(LongType()))\
    .withColumn('YEAR_PREDICT', lit(year)).drop('YEAR')
    
    
    if year == 2014:
        orders_ngt = orders_ngt_cur
    else:
        prev_yr_cust = orders_ngt_c.select( col('PARTYID'), ).distinct()\
                                    .join(orders_ngt_cur.select( col('PARTYID')).distinct(), ['PARTYID'], 'leftanti')\
                                    .withColumn('DATE_PREDICT', date_from_parts(year, 1, 1))\
                                    .withColumn('PREDICT_QUARTER', lit(1))\
                                    .withColumn('YEAR_PREDICT', lit(year))
        orders_ngt_cur = orders_ngt_cur.union(prev_yr_cust)
        
        orders_ngt = orders_ngt.union(orders_ngt_cur)
    orders_ngt_c = orders_ngt.cache_result()

w_ngt = Window.partitionBy('PARTYID').orderBy(col('DATE_PREDICT').asc())
orders_ngt = orders_ngt_c.withColumn("RN", row_number().over(w_ngt)).withColumn('LABEL', lit(0))



orders_ngt = orders_ngt.withColumn('BPN', lit(None)).withColumn('GUID', lit(None))\
                        .withColumn('SHOPPARTYID', lit(None)).withColumn('SHOPGUID', lit(None))\
                        .select(col('BPN'), col('GUID'), col('PARTYID'), col('SHOPPARTYID'), col('SHOPGUID'), col('DATE_PREDICT'), col('PREDICT_QUARTER'), col('YEAR_PREDICT'), col('RN'), col('LABEL'))
                                                            


df_DI_upgrades.columns, orders_ngt.columns


df_train = df_DI_upgrades.union(orders_ngt)


orders_to_predict = all_orders.withColumnRenamed("BPN", 'BPN_all').withColumnRenamed("GUID", 'GUID_all')\
                                .withColumnRenamed("SHOPPARTYID", 'SHOPPARTYID_all')\
                                .withColumnRenamed("SHOPGUID", 'SHOPGUID_all').drop(['PREDICT_QUARTER'])\
                                .join(df_train, ['PARTYID'], 'right')
orders_to_predict = orders_to_predict.withColumn("GAP_DAY", datediff("day", to_date(orders_to_predict.COMPLETEDATE), to_date(orders_to_predict.DATE_PREDICT))).filter(col('GAP_DAY') >= 0)



orders_to_predict = orders_to_predict.withColumn('BPN', when(col('LABEL')==0, col('BPN_all')).otherwise(col('BPN')))
orders_to_predict = orders_to_predict.withColumn('GUID', when(col('LABEL')==0, col('GUID_all')).otherwise(col('GUID')))
orders_to_predict = orders_to_predict.withColumn('SHOPGUID', when(col('LABEL')==0, col('SHOPGUID_all')).otherwise(col('SHOPGUID')))
orders_to_predict = orders_to_predict.withColumn('SHOPPARTYID', when(col('LABEL')==0, col('SHOPPARTYID_all')).otherwise(col('SHOPPARTYID')))


w = Window.partitionBy('PARTYID', 'RN', 'LABEL').orderBy(col('COMPLETEDATE').desc())

df_last = orders_to_predict.filter(col("BASE_MODEL").isNotNull()).filter(col('GAP_DAY') >= 1)            .withColumn("RN_",row_number().over(w)).filter(col("rn_") == 1).drop("rn_")

df_last = df_last.select(col('BPN'), col('GUID'), col('PARTYID'), col('SHOPPARTYID'), col('SHOPGUID'), col('DATE_PREDICT'), col('YEAR_PREDICT'), col('RN'), col('LABEL'), col('ITEM_SKU'), col('ITEM_PRICE'), col('BASE_MODEL'), col('TIER'), col('GAP_DAY'), col('COMPLETEDATE'))


df_DI = orders_to_predict.filter(col("BASE_MODEL").isNotNull()).filter(col('GAP_DAY') >= 1)
df_DI = df_DI.groupBy('PARTYID', 'RN', 'LABEL').count().withColumnRenamed("COUNT", "DI_PURCHASED")        .filter(col('DI_PURCHASED') > 0)
df_last = df_last.join(df_DI, ['PARTYID', 'RN', 'LABEL'], 'inner')


w2 = Window.partitionBy(['PARTYID', 'YEAR_PREDICT']).orderBy(col("RN"))
df_last = df_last.withColumn("row",row_number().over(w2)).filter(col("row") == 1).drop("row")


df_loyal = session.table('TR_SHOP_LOYALTY')
df_last = df_last.join(df_loyal.select(col('BPN'), col('SHOPGUID'), col('LOYALTY')), ['BPN', 'SHOPGUID'], 'left')

df_last = df_last.na.fill({"LOYALTY": 0})
df_last = df_last.join(all_orders.select(col('BPN'), col('COUNTRY')).drop_duplicates(), ['BPN'], 'left')

df_last = df_last.withColumn('SERIALNUMBER_FUZZY', lit(None))
df_last = df_last.withColumn('LAST_UPDATE_DISTANCE', lit(None))
df_last = df_last.withColumn('UPDATE_COUNT', lit(None))
df_last = df_last.withColumn('UPDATE_FREQUENCY', lit(None))
df_last = df_last.withColumn('HAS_SERIAL', lit(None))


df_last = df_last.withColumn('IDX', row_number().over(Window.orderBy(lit(1))))


df_last.write.mode("overwrite").save_as_table("TR_FT_TRAIN")


df_last.drop(['SERIALNUMBER_FUZZY', 'LAST_UPDATE_DISTANCE', 'UPDATE_COUNT', 'UPDATE_FREQUENCY', 'HAS_SERIAL', 'LOYALTY', 'COUNTRY'])

df_train = df_train.drop(['BPN', 'GUID', 'SHOPPARTYID', 'SHOPGUID']).join(df_last.select(col('BPN'), col('GUID'), col('SHOPPARTYID'), col('SHOPGUID'), col('PARTYID'), col('RN'), col('LABEL')), ['PARTYID', 'RN', 'LABEL'], 'inner')


orders_2years = orders_to_predict.filter(col('GAP_DAY') < 4 * 90).filter(col('GAP_DAY') >= 1)
orders_2years = orders_2years.withColumn('GAP_DAY', floor(col('GAP_DAY') / 90)).withColumnRenamed("GAP_DAY", 'GAP')\
                                .filter(col("BASE_MODEL").isNull())\
                                .filter(col('SHORT').isin(['TS', 'PT', 'HT', 'ES', 'DI']))\
                                .groupBy('PARTYID', 'RN', 'LABEL', 'SHORT', 'MAJOR', 'GAP')\
                                .agg(sum(col('ITEM_PRICE')).alias("PRICE"))

orders_2years = orders_2years.select("*", concat(col('SHORT'), lit('_'), col('MAJOR'), lit('_'), col('GAP')).alias('PRODUCT_LINE')).drop(["SHORT", "MAJOR", "GAP"])



product_lines = []
for i in orders_2years.select(col("PRODUCT_LINE")).distinct().collect():
    product_lines.append(i[0])
    
col_list = ["PARTYID", "RN", 'LABEL']
col_list.extend(product_lines)

df_line = orders_2years.pivot("PRODUCT_LINE", product_lines).sum("PRICE")
col_map = dict(zip(df_line.columns, col_list))
df_line = df_line.select([col(c).alias(col_map[c]) for c in df_line.columns])
df_line = df_line.na.fill(value=0, subset=product_lines)

col_list = ["PARTYID", "RN", 'LABEL']
col_list.extend(sorted(product_lines))
df_line = df_line.select(col_list)



df_line.write.mode("overwrite").save_as_table("TR_FT_PRODUCT_LINE")


orders_to_predict = all_orders.drop(['GUID', 'PREDICT_QUARTER', 'SHOPPARTYID'])\
                                .withColumnRenamed("PARTYID", 'PARTYID_IN_SHOP')\
                                .join(df_train, ['BPN', 'SHOPGUID'], 'right')\
                                .withColumn("GAP_DAY", datediff("day", to_date(orders_to_predict.COMPLETEDATE), to_date(orders_to_predict.DATE_PREDICT)))


df_mechs = orders_to_predict.filter(col('GAP_DAY') < 4 * 90).filter(col('GAP_DAY') >= 1)
df_mechs = df_mechs.withColumn('GAP_DAY', floor(col('GAP_DAY') / 90)).withColumnRenamed("GAP_DAY", 'GAP')\
                    .select(col('PARTYID'), col('BPN'), col('SHOPGUID'), col('PARTYID_IN_SHOP'),                                col('RN'), col('LABEL'), col('GAP')).distinct()

df_mechs = df_mechs.groupBy('PARTYID', 'BPN', 'SHOPGUID', 'RN', 'LABEL', 'GAP').count()\
                    .withColumnRenamed("COUNT", "TECH")
df_mechs = df_mechs.pivot('GAP', [0,1,2,3]).sum("TECH")



col_list = ['PARTYID', 'BPN', 'SHOPGUID', 'RN', 'LABEL', 'TECH_0', 'TECH_1', 'TECH_2', 'TECH_3']

col_map = dict(zip(df_mechs.columns, col_list))
df_mechs = df_mechs.select([col(c).alias(col_map[c]) for c in df_mechs.columns])
df_mechs = df_mechs.na.fill(value=0, subset=['TECH_0', 'TECH_1', 'TECH_2', 'TECH_3'])


df_mechs.write.mode("overwrite").save_as_table("TR_FT_MECHANICS")

