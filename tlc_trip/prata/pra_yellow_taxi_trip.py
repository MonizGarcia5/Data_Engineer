# Databricks notebook source
# MAGIC %md ##Carga dados Yellow Taxi Trip - Camada Prata

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType

# 🔹 Cria a sessão Spark
spark = SparkSession.builder.getOrCreate()

# 🔹 Criar schema se não existirem na camada prata
spark.sql("CREATE CATALOG IF NOT EXISTS tlc_trip")
spark.sql("CREATE SCHEMA IF NOT EXISTS tlc_trip.prata")

# Ler dados da bronze (todos como string)
df_bronze = spark.read.format("delta").load("tlc_trip.bronze.yellow_taxi_trip")

# Definição do schema com tipos corretos
schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])

# Lê os dados da camada bronze (presumindo que são todos strings)
df_bronze = spark.read.format("delta").table("tlc_trip.bronze.yellow_taxi_trip")

# Aplica as conversões usando cast em cada coluna
df_silver = df_bronze.select(
    col("VendorID").cast(LongType()).alias("VendorID"),
    col("tpep_pickup_datetime").cast(TimestampType()).alias("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime").cast(TimestampType()).alias("tpep_dropoff_datetime"),
    col("passenger_count").cast(DoubleType()).alias("passenger_count"),
    col("trip_distance").cast(DoubleType()).alias("trip_distance"),
    col("RatecodeID").cast(DoubleType()).alias("RatecodeID"),
    col("store_and_fwd_flag").alias("store_and_fwd_flag"),  # já string
    col("PULocationID").cast(LongType()).alias("PULocationID"),
    col("DOLocationID").cast(LongType()).alias("DOLocationID"),
    col("payment_type").cast(LongType()).alias("payment_type"),
    col("fare_amount").cast(DoubleType()).alias("fare_amount"),
    col("extra").cast(DoubleType()).alias("extra"),
    col("mta_tax").cast(DoubleType()).alias("mta_tax"),
    col("tip_amount").cast(DoubleType()).alias("tip_amount"),
    col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
    col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
    col("total_amount").cast(DoubleType()).alias("total_amount"),
    col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
    col("airport_fee").cast(DoubleType()).alias("airport_fee"),
)

# Gravar o dataframe silver no Delta novamente
df_silver.write.format("delta").mode("overwrite").saveAsTable("tlc_trip.prata.yellow_taxi_trip")


# COMMAND ----------

# MAGIC %md ##Tratamento dos dados 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, upper, when, year, month, dayofmonth
import requests
from pyspark.sql import SparkSession

# 🔹 Cria a sessão Spark
spark = SparkSession.builder.getOrCreate()

# Carregar dados da camada prata já tipados
df = spark.table("tlc_trip.prata.yellow_taxi_trip")

#  Remover registros inconsistentes
df_clean = df.filter(
    (col("tpep_pickup_datetime").isNotNull()) &
    (col("VendorID").isNotNull()) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0) &
    (col("fare_amount") >= 0)
)

# Gerar DataFrame somente com os registros removidos
df_removed = df.subtract(df_clean)

# Normalizar colunas
df_clean = df_clean.withColumn(
    "store_and_fwd_flag", upper(col("store_and_fwd_flag"))
)

# Colunas derivadas para facilitar análises temporais
df_clean = df_clean.withColumn("pickup_year", year(col("tpep_pickup_datetime")))
df_clean = df_clean.withColumn("pickup_month", month(col("tpep_pickup_datetime")))
df_clean = df_clean.withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime")))

# Validar regra simples de negócio
df_clean = df_clean.withColumn(
    "total_amount_check",
    (col("total_amount") >= (col("fare_amount") + col("tip_amount") + col("tolls_amount") + col("extra")))
)

# Filtrar inválidos
df_invalid = df_clean.filter(col("total_amount_check") == False)

# Filtrar válidos excluindo os inválidos
df_valid = df_clean.filter(col("total_amount_check") == True)

print(f"Total original: {df.count()}")
print(f"Registros válidos: {df_valid.count()}")
print(f"Registros inválidos: {df_invalid.count()}")
print(f"Registros removidos: {df_removed.count()}")

print(f"Soma válidos + inválidos + removidos = {df_valid.count() + df_invalid.count() + df_removed.count()}")

# Gravar os dados tratados e válidos na camada prata
df_valid.write.format("delta").mode("overwrite").saveAsTable("tlc_trip.prata.yellow_taxi_trip_tratada")

# Gravar os dados inválidos na camada prata
df_invalid.write.format("delta").mode("overwrite").saveAsTable("tlc_trip.prata.yellow_taxi_trip_invalida")

# Gravar os dados removidos na camada prata
df_removed.write.format("delta").mode("overwrite").saveAsTable("tlc_trip.prata.yellow_taxi_trip_removido")
