# Databricks notebook source
# Objetivo : Criar a tabela fa_venda na Base de dados Gold (Databricks)

# A tabela fa_venda a ser criada tem como origem a tabela di_venda armazenada 
# na base de dados Silver (DataBricks)

# Bibliotecas utilizadas  
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
from delta.tables import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Table Producao")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

#Nome dos databases.
data_base_silver="db_silver" 
data_base_gold="db_gold" 

#Nome da tabela
table="fa_venda"

#Chave para utlizar  upsert do Merge
primarykey="codigo_produto" 

df = spark.sql(""" select codigo_produto,codigo_regiao,data_venda,sum(qtde_venda) qtde_venda ,sum(valor_venda) valor_venda 
                 from {}.{}  group by codigo_produto,codigo_regiao,data_venda  """.format(data_base_silver,table)) 

#altera type schema
df = df.withColumn("data_atualizacao",current_timestamp().cast(TimestampType())) \
       .withColumn("id_data_venda",date_format("data_venda","yyyyMMdd").cast(LongType()))

columns_to_drop = ['data_venda']
df = df.drop(*columns_to_drop)
      
#Read Catolog List
tables = [t.name for t in spark.catalog.listTables(data_base_gold)]

if table in tables:
    #Read Delta Table
    deltaTable = DeltaTable.forName(spark, "{}.{}".format(data_base_gold,table))
    deltaTable.delete()
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