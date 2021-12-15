# Databricks notebook source
# Objetivo : Criar a tabela fa_venda na Base de dados Gold (Databricks)

# A tabela de Vendas tem como origem um arquivo no formato CSV (Vendas.CSV)
# que está armazenado no DBFS que é o sistema de arquivos Databricks.

# O arquivo Vendas.CSV  contém as colunas :
#  -CODIGO_PRODUTO
#  -CODIGO_REGIAO
#  -DATA_VENDA
#  -QTDE_VENDA
#  -VALOR_VENDA

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

# Criar a tabela fa_venda na base de dados Gold (Datbricks)    
table="fa_venda"


file= """"/FileStore/tables/bronze/VENDAS.csv",header="true",delimiter=";")"""
create_table(data_base,table,file)