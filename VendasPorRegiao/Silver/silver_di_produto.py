# Databricks notebook source
# Objetivo : Criar a tabela di_produto na Base de dados Silver (Databricks)

# A tabela di_produto a ser criada tem como origem a tabela di_produto armazenada 
# na base de dados Bronze (DataBricks)

# Bibliotecas utilizadas  
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

#Nome do database.
data_base_bronze="db_bronze" 
data_base_silver="db_silver" 

#Nome da tabela
table="di_produto"

#Chave para utlizar  upsert do Merge
primarykey="codigo_produto" 

df = spark.sql(" select codigo_produto,descricao_produto,sigla_produto from {}.{}".format(data_base_bronze,table)) 

#altera type schema
df = df.withColumn("data_atualizacao",current_timestamp().cast(TimestampType())) \
       .withColumn("codigo_produto",col("codigo_produto").cast(LongType()))
  
#Read Catolog List
tables = [t.name for t in spark.catalog.listTables(data_base_silver)]

if table in tables:
    #Read Delta Table
    deltaTable = DeltaTable.forName(spark, "{}.{}".format(data_base_silver,table))
    deltaTable.alias("t") \
               .merge(df.alias("s"), "t.{} = s.{}".format(primarykey,primarykey)) \
               .whenMatchedUpdateAll()\
               .whenNotMatchedInsertAll()\
               .execute()      
else:   
    df.write \
          .format("delta") \
          .mode("overwrite") \
          .saveAsTable("{}.{}".format(data_base_silver,table))