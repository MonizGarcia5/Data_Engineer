# Databricks notebook source
# Objetivo : Criar as base de dados no ambiente Databricks para as camadas Gold,Silver e Bronze

# Biblioteca Spark 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark and Hive")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

# Função para criação de banco de dados no ambiente Databricks
def create_db(db_name):
    query_db="CREATE DATABASE  IF NOT EXISTS {}".format(db_name)
    spark.sql(query_db)

# Cria banco de dados na camada Gold
data_base="db_gold" 
create_db(data_base)
   
# Cria banco de dados na camada Silver
data_base="db_silver" 
create_db(data_base)

# Cria banco de dados na camada Bronze
data_base="db_bronze" 
create_db(data_base)

