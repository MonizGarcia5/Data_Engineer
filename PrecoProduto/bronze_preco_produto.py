# Databricks notebook source
# Objetivo : Criar a tabela fa_preco_produto na Base de dados Bronze (Databricks)

# A tabela de preco de produto tem como origem um arquivo no formato TXT (PRECO_DE_PRODUTO.TXT)
# com as colunas organizadas de forma posicional, armazenado no DBFS do Databricks.

# O arquivo PRECO_DE_PRODUTO.CSV  contém as colunas :
#  - CODIGO_PRODUTO
#  - DESCRICAO_PRODUTO
#  - DATA ATUALIZACAO
#  - VALOR PRECO PRODUTO .

from delta import *
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Spark and Hive")\
                                .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
                                .config("spark.speculation", "false").config("hive.exec.dynamic.partition", "true")\
                                .config("hive.exec.dynamic.partition.mode", "nonstrict")\
                                .enableHiveSupport()\
                                .getOrCreate()

# Criar a tabela fa_preco_produto na camada Bronze (Datbricks)    
base_dados="db_bronze" 
table="fa_preco_produto"

primarykey ="cd_produto"

# Nome do arquivo texto 
file_name= "/FileStore/tables/bronze/PRECO_DE_PRODUTO.TXT"

# Abre arquivo e carrega os dados
logData = spark.read.text(file_name)
logDataLines = logData.collect()

vals  =[]

# le os valores das coluna por posicao
for line in logDataLines:
    words = line
    for word in words:
        regline = [word[0:10],word[15:60],word[116:118]+"-"+word[118:120]+"-"+word[120:124],word[125:135]]
        vals.append(regline)

columns = ['CD_PRODUTO','DS_PRODUTO','ATUALIZACAO', 'PRECO_PRODUTO']

# Cria o  DataFrame
df = spark.createDataFrame(vals, columns)

# Convert valor de venda produto 
df=df.withColumn("vl_preco_produto",concat(concat(substring(col("PRECO_PRODUTO"),0,8),lit(".")),\
                                                  substring(col("PRECO_PRODUTO"),9,2)).cast(DecimalType(15,2)))

# Convert data ataulizacao
df=df.withColumn("dt_atualizacao",to_date(col("ATUALIZACAO"),"dd-MM-yyyy"))  

#data inclusao
df = df.withColumn("dt_inclusao",current_timestamp())        

#Converte nome das colunas  para minusculo
for col in df.columns:
    df = df.withColumnRenamed(col, col.lower())

#Le o catalogo 
tables = [t.name for t in spark.catalog.listTables(base_dados)]

# Grava dados no fromato Delta 
if table in tables:
    deltaTable = DeltaTable.forPath(spark, "/FileStore/tables/bronze/{}/{}/".format(base_dados,table))
    deltaTable.delete()
    deltaTable.alias("t") \
            .merge(df.alias("s"), "s.{} = t.{}".format(primarykey,primarykey)) \
            .whenNotMatchedInsertAll()\
            .execute()                 
else:   
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("{}.{}".format(base_dados,table), path="/FileStore/tables/bronze/{}/{}/".format(base_dados,table))  