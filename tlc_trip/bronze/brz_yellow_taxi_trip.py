# Databricks notebook source
# MAGIC %md ##Carga dados Yellow Taxi Trip - Camada Bronze

# COMMAND ----------

import requests
from pyspark.sql import SparkSession

# 🔹 Cria a sessão Spark
spark = SparkSession.builder.getOrCreate()

# 🔹 Criar catálogo e schema se não existirem na camada bronze
spark.sql("CREATE CATALOG IF NOT EXISTS tlc_trip")
spark.sql("CREATE SCHEMA IF NOT EXISTS tlc_trip.bronze")

# 🔹 ID do dataset "Yellow Taxi Trip Data" (2023) no NYC Open Data
dataset_id = "4b4i-vvec"

# 🔹 URL base da API SODA do Socrata
base_url = f"https://data.cityofnewyork.us/resource/{dataset_id}.json"

# 🔹 Filtro de data (apenas viagens de março/2023)
# Usando a sintaxe SoQL no parâmetro "$where"
where_clause = "tpep_pickup_datetime between '2023-01-01T00:00:00' and '2023-04-30T23:59:59'"

# 🔹 Configuração de paginação
limit = 50000   # máximo de registros por requisição (Socrata aceita até 50k)
offset = 0      # posição inicial para buscar os dados

# Lista para acumular DataFrames parciais
all_dfs = []

# 🔄 Loop de paginação até trazer todos os registros
while True:
    # Parâmetros da requisição para a API
    params = {
        "$where": where_clause,  # filtro de data
        "$limit": limit,         # quantidade de registros por requisição
        "$offset": offset        # deslocamento para próxima página
    }
    
    print(f"🔄 Baixando registros {offset} até {offset + limit} ...")
    
    # Faz a requisição HTTP GET
    response = requests.get(base_url, params=params)
    
    # Converte o retorno JSON para objeto Python
    data = response.json()
    
    # Se não houver mais dados, encerra o loop
    if not data:
        break
    
    # Converte lista de dicionários para DataFrame Spark
    df_temp = spark.createDataFrame(data)
    
    # Adiciona o DataFrame parcial à lista
    all_dfs.append(df_temp)
    
    # Avança para próxima "página" de registros
    offset += limit

# 🔹 Une todos os DataFrames em um único
if all_dfs:
    # Começa com o primeiro DataFrame
    df_final = all_dfs[0]
    
    # Faz união com os demais DataFrames (por nome de coluna)
    for df in all_dfs[1:]:
        df_final = df_final.unionByName(df)
    
    # Mostra o total de registros carregados
    print(f"✅ Total de registros carregados: {df_final.count()}")

# Salvar a camada Bronze unificada
df_final.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("tlc_trip.bronze.yellow_taxi_trip")
