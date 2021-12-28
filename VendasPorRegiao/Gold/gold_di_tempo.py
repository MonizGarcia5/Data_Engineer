# Databricks notebook source
# Objetivo : Criar a tabela fa_venda na Base de dados Gold (Databricks)

# Os dados da tabela di_tempo  serão gravados na base de dados Silver (DataBricks)
# considerando um intervalo de datas entre 01-janeiro-2001 até 01-janeiro-2030

# Bibliotecas utilizadas  
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TimeTable")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

#Nome do database.
data_base_silver="db_silver" 

#Nome da tabela
table="di_tempo"

primarykey="id_data"

#alter type schema
def generate_series(start, stop, interval):
    """
    :param start  - lower bound, inclusive
    :param stop   - upper bound, exclusive
    :interval int - increment interval in seconds
    """
    spark = SparkSession.builder.getOrCreate()
    # Determine start and stops in epoch seconds
    start, stop = spark.createDataFrame(
        [(start, stop)], ("start", "stop")
    ).select(
        [col(c).cast("timestamp").cast("long") for c in ("start", "stop")
    ]).first()
    # Create range with increments and cast to timestamp
    return spark.range(start, stop, interval).select(
        col("id").cast("timestamp").alias("data")
    )

df=generate_series("2000-01-01", "2030-01-01", 60 * 60 * 24)

df = df.withColumn("id_data",date_format(df['data'],"yyyyMMdd").cast(LongType()))\
       .withColumn("dia",date_format(df['data'],"dd").cast(LongType()))\
       .withColumn("mes",date_format(df['data'],"MM").cast(LongType()))\
       .withColumn("ano",date_format(df['data'],"yyyy").cast(LongType()))\
       .withColumn("mesano",date_format(df['data'],"MMyyyy").cast(LongType()))\
       .withColumn("dt_data_update",current_timestamp().cast(TimestampType())) 

#Read Catolog List

tables = [t.name for t in spark.catalog.listTables(data_base_gold)]

if table in tables:
    #Read Delta Table
    deltaTable = DeltaTable.forName(spark, "{}.{}".format(data_base_gold,table))
    #Read min date
    deltaTable.alias("t") \
               .merge(df.alias("s"), "t.{} = s.{}".format(primarykey,primarykey)) \
               .whenMatchedUpdateAll()\
               .whenNotMatchedInsertAll()\
               .execute()                 
else:   
    df.write \
          .format("delta") \
          .mode("overwrite") \
          .saveAsTable("{}.{}".format(data_base_gold,table))