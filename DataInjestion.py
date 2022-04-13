from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType, DecimalType
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from typing import List
import json
from datetime import datetime



#r'C:\Users\Keval\Desktop\AirflowDocker\logs\marketvol\**\**\*.log'
#f"/home/airflow/{variable}.csv"



spark = SparkSession.builder.master('local').appName('app').getOrCreate()



schema = StructType([
    StructField("trade_dt",StringType(),True),
    StructField("rec_type",StringType(),True),
    StructField("symbol",StringType(),True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", StringType(), True),
    StructField("event_seq_nb", StringType(), True),
    StructField("arrival_tm", StringType(), True),
    StructField("trade_pr", StringType(), True),
    StructField("bid_pr", StringType(), True),
    StructField("bid_size", StringType(), True),
    StructField("ask_pr", StringType(), True),
    StructField("ask_size", StringType(), True),
    StructField("partition", StringType(), True),
    StructField("line", StringType(), True)
  ])



def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    
    try:
        # [logic to parse records]
     if record[record_type_pos] == "T":
         event = (record[0], record[2], record[3], record[6], record[1], record[5], record[4], record[7], 0.0, 0, 0.0, 0, "T", "")
         return event
     elif record[record_type_pos] == "Q":
         event = (record[0], record[2], record[3], record[6], record[1], record[5], record[4], 0.0, record[7], record[8], record[9], record[10], "Q", "")
         return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return (None, None, None, None, None, None, None, None, None, None, None, None, "B", line)


raw = spark.sparkContext.textFile("data/csv/2020-08-06/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(data=parsed, schema=schema)


def parse_json(line:str):
    record = json.loads(line)
    record_type = record['event_type']

    try:
        # [logic to parse records]
        if record_type == "T":
            if len(record.keys()) == 10:
                event = (
                    record['trade_dt'],
                    record['event_type'],
                    record['symbol'],
                    record['exchange'],
                    record['event_tm'],
                    record['event_seq_nb'],
                    record['file_tm'],
                    record['price'],
                    0.0,
                    0,
                    0.0,
                    0,
                    "T",
                    "")
            else :
                event = (None, None, None, None, None, None, None, None, None, None, None, None, "B", line)
            return event
        if record_type == "Q":
            # [Get the applicable field values from json]
            if len(record.keys()) == 11:
                event = (
                    record['trade_dt'],
                    record['event_type'],
                    record['symbol'],
                    record['exchange'],
                    record['event_tm'],
                    record['event_seq_nb'],
                    record['file_tm'],
                    0.0,
                    record['bid_pr'],
                    record['bid_size'],
                    record['ask_pr'],
                    record['ask_size'],
                    "Q",
                    "")
            else :
                event = (None, None, None, None, None, None, None, None, None, None, None, None, "B", line)
            return event
    except Exception as e:
        # [save record to dummy event in bad partition]
        # [fill in the fields as None or empty string]
        return (None, None, None, None, None, None, None, None, None, None, None, None, "B", line)


raw = spark.sparkContext.textFile("data/json/2020-08-06/NASDAQ/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.txt")
parsed = raw.map(lambda line: parse_json(line))
data = spark.createDataFrame(data=parsed, schema=schema)


data.write.partitionBy("partition").mode("overwrite").parquet("data/output_dir")


def applyLatest(trade):
    trade_grouped = trade.orderBy("arrival_tm").groupBy('trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb') \
        .agg(F.collect_set("arrival_tm").alias("arrival_tm"))
    trade_rm_dp = trade_grouped.withColumn("arrival_tm", F.slice(trade_grouped["arrival_tm"], 1, 1)[0])
    return trade_rm_dp.drop(F.col('arrival_tm'))


trade_common = spark.read.parquet("data/output_dir/partition=T")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm',
'event_seq_nb', 'arrival_tm', 'trade_pr')
trade_corrected = applyLatest(trade)

# print(trade_corrected.show())
trade_date = datetime.now()
trade.write.mode('overwrite').parquet(f'data/data_load/traded_date={trade_date.strftime("%Y-%m-%d")}')


trade_common = spark.read.parquet("data/output_dir/partition=Q")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm',
'event_seq_nb', 'arrival_tm', 'trade_pr')
trade_corrected = applyLatest(trade)

# print(trade_corrected.show())
trade_date = datetime.now()
trade.write.mode('overwrite').parquet(f'data/data_load/traded_date={trade_date.strftime("%Y-%m-%d")}')


trade_common = spark.read.parquet("data/output_dir/partition=B")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm',
'event_seq_nb', 'arrival_tm', 'trade_pr')
trade_corrected = applyLatest(trade)

# print(trade_corrected.show())
trade_date = datetime.now()
trade.write.mode('overwrite').parquet(f'data/data_load/traded_date={trade_date.strftime("%Y-%m-%d")}')