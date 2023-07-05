
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



train = session.table("TR_FT_TRAIN")
serials = session.sql('''WITH p as (SELECT t.PARTYID, t.DATE_PREDICT, d.COMPLETEDATE, d.SERIALNUMBER_FUZZY, t.IDX,
                                            ROW_NUMBER() OVER (PARTITION BY t.PARTYID, t.DATE_PREDICT 
                                                                ORDER BY d.COMPLETEDATE) as dt_idx
                                FROM TR_FT_TRAIN t
                                LEFT JOIN (SELECT * FROM TR_ORDERS_DEVICE WHERE SERIALNUMBER_FUZZY is not null) d 
                                    ON t.PARTYID = d.PARTYID
                                WHERE t.DATE_PREDICT > d.COMPLETEDATE OR d.SERIALNUMBER_FUZZY IS NULL)

                        SELECT SERIALNUMBER_FUZZY, IDX
                        FROM p
                        WHERE (PARTYID, DATE_PREDICT, dt_idx) IN (
                                SELECT PARTYID, DATE_PREDICT, MIN(dt_idx) AS min_idx
                                FROM p
                                GROUP BY PARTYID, DATE_PREDICT)''')

use_df = session.table("TR_DEVICESTATUSHISTORY")


train.count(), serials.count(), use_df.count()


train = train.drop(['SERIALNUMBER_FUZZY', 'LAST_UPDATE_DISTANCE', 'UPDATE_COUNT', 'UPDATE_FREQ', 'HAS_SERIAL'])


train = train.join(serials, on="IDX", how="left").select(col('PARTYID'), col('DATE_PREDICT'), col('YEAR_PREDICT'), col('ITEM_SKU'), col('ITEM_PRICE'), col('BASE_MODEL'), col('TIER'), col('GAP_DAY'), col('COMPLETEDATE'), col('DI_PURCHASED'), col('SERIALNUMBER_FUZZY'), col('IDX'))
train = train.cache_result()



use_df = use_df.join(serials, use_df.SERIALNUMBER==serials.SERIALNUMBER_FUZZY, how='inner').drop(["IDX", "SERIALNUMBER_FUZZY"])

# Filter rows based on 'BUNDLENUMBER' column
bundle_numbers = ['22.2.0', '22.4', '22.4.0', '20.2.0', '21.2.0', '20.4.0', '21.4.0',
       '22.2', '19.4.0', '16.2', '17.2', '18.4', '18.2', '15.2', '15.4',
       '16.4', '19.2',  '18.2.0', '18.4.0', '19.2.0', 
       '17.4.0', '18.2.1', '17.4', '19.4.2', '18.4.1', '20.4.2', '19.4',
       '21.4', '19.2.1', '20.2.2', '20.2', '20.2.1', '21.2.1', '19.4.1',
       '21.2', '21.4.1', '19.2.2', '17.2.0', '15.4.0', '16.4.0', '20.4', '16.2.0', '15.2.0']
use_df = use_df.filter(col('BUNDLENUMBER').isin(bundle_numbers))

# Update 'BUNDLENUMBER' column values
use_df = use_df.withColumn('BUNDLENUMBER', concat(lit('20'), split(col('BUNDLENUMBER'), lit('.'))[0], lit('.'), split(col('BUNDLENUMBER'), lit('.'))[1]))


# Update 'y1' column
use_df = use_df.withColumn('y1', use_df['BUNDLEUPDATEDATE'].substr(1, 4).cast('integer'))

# Update 'y2' column
use_df = use_df.withColumn('y2', use_df['BUNDLENUMBER'].substr(1, 4).cast('integer'))

# Filter rows based on conditions
use_df = use_df.filter((use_df['y1'] == use_df['y2']) | (use_df['y1'] - 1 == use_df['y2']))

# Update 'BUNDLEUPDATEDATE' column
use_df = use_df.withColumn('BUNDLEUPDATEDATE', use_df['BUNDLEUPDATEDATE'].substr(1, 10))

use_df = use_df.select('SERIALNUMBER', 'BUNDLEUPDATEDATE', 'y1', 'BUNDLENUMBER')

use_df = use_df.cache_result()


df2 = train.select(col('DATE_PREDICT'), col('SERIALNUMBER_FUZZY'), col('IDX'))



merge_t = df2.join(use_df, df2.SERIALNUMBER_FUZZY == use_df.SERIALNUMBER, how = 'left')
m = merge_t.filter(merge_t['DATE_PREDICT'] > merge_t['BUNDLEUPDATEDATE'])


m2 = m.dropDuplicates(['IDX', 'SERIALNUMBER_FUZZY', 'DATE_PREDICT', 'BUNDLENUMBER'])


update_count = m2.group_by('IDX').count().select(col('IDX'), col('COUNT').alias('UPDATE_COUNT'))


df2 = df2.join(update_count, on="IDX", how="left")


#df2.filter((df2['SERIALNUMBER_FUZZY'].isNotNull()) & (df2['UPDATE_COUNT'].isNull())).count()


last_update_date = m2.select(col('IDX'), col('BUNDLEUPDATEDATE')).group_by(['IDX']).agg(max('BUNDLEUPDATEDATE').alias('UD'))

df2 = df2.join(last_update_date, on="IDX", how="left")


df2 = df2.withColumn('LAST_UPDATE_DISTANCE', expr("datediff(day, to_date(substring(UD, 1, 10)), to_date(DATE_PREDICT))"))


old_update_date = m2.select(col('IDX'), col('BUNDLEUPDATEDATE')).group_by('IDX').agg(min('BUNDLEUPDATEDATE').alias('OD'))
df2 = df2.join(old_update_date, on="IDX", how="left")


df2 = df2.withColumn('UPDATE_FREQUENCY', expr("datediff(day, to_date(substring(OD, 1, 10)), to_date(DATE_PREDICT))"))


df2 = df2.cache_result()


train = train.join(df2.select(col('IDX'), col('UPDATE_COUNT'), col('LAST_UPDATE_DISTANCE'), col('UPDATE_FREQUENCY')), on="IDX", how="left")


train = train.withColumn('HAS_SERIAL', when(col('SERIALNUMBER_FUZZY').isNotNull(), 1).otherwise(0))


train.filter((train['SERIALNUMBER_FUZZY'].isNotNull()) & (train['LAST_UPDATE_DISTANCE'].isNull())).count()


train.count()


sn_table = train.select(col('IDX'), col('SERIALNUMBER_FUZZY'), col('UPDATE_COUNT'), col('LAST_UPDATE_DISTANCE'), col('UPDATE_FREQUENCY'), col('HAS_SERIAL'))



sn_table.write.mode("overwrite").save_as_table("TR_SN_FEATURES")


a = session.sql('''UPDATE DATA_LAB_TEST.PREDICTOR.TR_FT_TRAIN A
    SET A.SERIALNUMBER_FUZZY = B.SERIALNUMBER_FUZZY,
        A.UPDATE_COUNT = B.UPDATE_COUNT,
        A.LAST_UPDATE_DISTANCE = B.LAST_UPDATE_DISTANCE,
        A.UPDATE_FREQUENCY = B.UPDATE_FREQUENCY,
        A.HAS_SERIAL = B.HAS_SERIAL
    FROM DATA_LAB_TEST.PREDICTOR.TR_SN_FEATURES B
    WHERE A.IDX = B.IDX''')


a.collect()

