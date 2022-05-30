#!/usr/bin/env python
# coding: utf-8

# ## PayPay Data Engineer Challenge - Raja Prakash Muthuraman May 30, 2022.

# import Necessary Modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import re

import warnings
warnings.filterwarnings('ignore')

# Create Spark Session

spark = SparkSession.Builder().appName('payPayApp') \
    .master('local[2]') \
    .config('spark.scheduler.mode', 'FAIR') \
    .config('spark.sql.shuffle.partitions', 4) \
    .getOrCreate()

# Read Log Data

logFilePath = "../data/2015_07_22_mktplace_shop_web_log_sample.log.gz"
df = spark.read.text(logFilePath)
df.printSchema()

# In[4]:


# Reg Ex Pattern and parse analytical data

v_new_regex = """(\S+) (\S+) (\S+):(\d+) (\S+):(\d+) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) (\d+) (\d+) "([^ ]*) ([^ ]*) (- |[^ ]*)" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$"""


# Function to parse each line with RegEx Pattern
def fnParseLogLineValue(logline, pattern=v_new_regex):
    value = logline.value
    match = re.search(pattern, value)

    parsedValues = []
    if match is None:
        parsedValues = [None for i in range(1, 20)]
    else:
        parsedValues = [match.group(i).strip() for i in range(1, 20)]

    returnValue = tuple(i for i in parsedValues)
    return returnValue


# Column Name
col_names = ['ts', 'domain_nm', 'fe_ip', 'fe_port', 'bcknd_ip', 'bcknd_port', 'fe_prcss_time', 'bcknd_prcss_time',
             'clnt_resp_tm', 'fe_resp_cd', 'bcknd_resp_cd', 'received_bytes', 'sent_bytes', 'rq_type', 'url',
             'protocol', 'usr_agnt', 'ssl', 'ssl2']

df_parsed = df.rdd.map(fnParseLogLineValue)
df_parsed = df_parsed.toDF(col_names)

# Adding additional calculative columns

v_max_active_ssn_seconds = 900  # i.e. 15 mins
v_window_fn = Window.partitionBy("fe_ip", "bcknd_ip")
v_window_time_diff = v_window_fn.orderBy(asc("ts"))
v_window_session = v_window_fn.orderBy([asc("fe_ip"), asc("bcknd_ip"), asc("time_spend")])

pvs_ts = lag("ts", 1).over(v_window_time_diff)
df_parsed = df_parsed.withColumn('ts', to_timestamp('ts')).withColumn('pvs_ts', pvs_ts).withColumn('time_spend',
                                                                                                   unix_timestamp(
                                                                                                       'ts') - unix_timestamp(
                                                                                                       'pvs_ts')).na.fill(
    0).withColumn('unique_ssn_flg',
                  when(col('time_spend') < lit(v_max_active_ssn_seconds), lit(0)).otherwise(lit(1))).withColumn(
    'ssn_id', sum(col('unique_ssn_flg')).over(v_window_session))

# Select only the columns needed for analytics

lst_of_cols = ['ts', "fe_ip", "bcknd_ip", 'pvs_ts', 'url', 'time_spend', 'unique_ssn_flg', 'ssn_id']
df_log = df_parsed.select(lst_of_cols)

df_log.to_pandas_on_spark().head()

# ### Log Analytics using Spark
# #### 1.Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.

# Group by IP and session ID and get distinct URLs

group_by_cols = ['fe_ip', 'ssn_id']
df_sessionise = df_log.groupBy(group_by_cols).agg(count('url').alias('ip_hits'))

df_sessionise.to_pandas_on_spark().head()

# #### 2. Determine the average session time


group_by_cols = ['fe_ip', 'ssn_id']

# Group by Ip and Session ID and difference the max timestamp with min timestamp and get average across all
df_avg_ssn_tm = df_log.groupBy(group_by_cols).agg(max('ts').alias('max_ts'), min('ts').alias('min_ts')).withColumn(
    'session_time', unix_timestamp('max_ts') - unix_timestamp('min_ts')).groupBy().agg(
    avg('session_time').alias('average_ssn_time'))

df_avg_ssn_tm.to_pandas_on_spark().head()

# #### 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

group_by_cols = ['fe_ip', 'ssn_id']

# Group By IP and Session and Get Unique URL page
df_unique_url = df_log.groupBy(group_by_cols).agg(count_distinct('url').alias('unique_url_per_ssn'))

df_unique_url.to_pandas_on_spark().head()

# #### 4.Find the most engaged users, ie the IPs with the longest session times

group_by_cols = ['fe_ip', 'ssn_id']

# Group By IP and session id and get Time difference of start and end of the session and get max of time Spend
df_long_ssn_ip = df_log.groupBy(group_by_cols).agg(max('ts').alias('max_ts'), min('ts').alias('min_ts')).withColumn(
    'session_time', unix_timestamp('max_ts') - unix_timestamp('min_ts')).groupBy('fe_ip').agg(
    max('session_time').alias('long_ssn_time_per_ip'))

df_long_ssn_ip.to_pandas_on_spark().head()

# End
spark.stop()
