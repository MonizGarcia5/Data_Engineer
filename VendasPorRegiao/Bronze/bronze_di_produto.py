# Databricks notebook source
# Objetivo : Criar a tabela di_produto na Base de dados Gold (Databricks)

# A tabela de produto tem como origem um arquivo no formato CSV (PRODUTO.CSV)
# que está armazenado no DBFS que é o sistema de arquivos Databricks.

# O arquivo PRODUTO.CSV  contém as colunas :
#  - CODIGO_PRODUTO
#  - DESCRICAO_PRODUTO
#  - SIGLA_PRODUTO .

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

# Criar a tabela di_produto na base de dados Gold (Datbricks)    
table="di_produto"
file= """"/FileStore/tables/bronze/PRODUTO.csv",header="true",delimiter=";")"""
create_table(data_base,table,file)

