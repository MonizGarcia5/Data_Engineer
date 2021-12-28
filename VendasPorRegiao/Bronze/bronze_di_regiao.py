# Databricks notebook source
# Objetivo : Criar a tabela di_regiao na Base de dados Gold (Databricks)

# A tabela de regiao tem como origem um arquivo no formato CSV (REGIAO.CSV)
# que está armazenado no DBFS que é o sistema de arquivos Databricks.

# O arquivo REGIAO.CSV  contém as colunas :
#  -CODIGO_REGIAO
#  -DESCRICAO_REGIAO
#  -NOME_PAIS

# Biblioteca Spark 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark and Hive")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

# Funcão utilizada na criação de tabelas
def create_table(db_name,tb_name,fl_name):
    query_tb="CREATE TABLE  IF NOT EXISTS {}.{}  USING csv OPTIONS (path {}".format(db_name,tb_name,fl_name)
    spark.sql(query_tb)

# Criar a tabela di_regiao na base de dados Gold (Datbricks)    
table="di_regiao"
file= """"/FileStore/tables/bronze/REGIAO.csv",header="true",delimiter=";")"""
create_table(data_base,table,file)