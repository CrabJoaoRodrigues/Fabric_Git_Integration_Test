# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e5fd557f-b734-49bb-a504-c5b4e62f8e32",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "a65c5a58-c2ad-443e-9174-fe5649ee908a",
# META       "known_lakehouses": [
# META         {
# META           "id": "e5fd557f-b734-49bb-a504-c5b4e62f8e32"
# META         },
# META         {
# META           "id": "3e1c0d50-1f56-4566-8c35-76e9fa99ac60"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# 1. Pedir ao Fabric a lista de TODAS as tabelas que existem no Bronze
lista_tabelas = spark.catalog.listTables("LH_test1")

# 2. Correr o processo automaticamente para cada uma delas
for tabela in lista_tabelas:
    nome_tabela = tabela.name


    if nome_tabela == "nyc_taxi_green" or nome_tabela == "dbo":
        print(f"[IGNORADA] A saltar a tabela/schema '{nome_tabela}'...")
        continue

    print(f"[PROCESSANDO] A iniciar limpeza da tabela: {nome_tabela}")


    # Leitura dinâmica usando o nome da variável
    df_bronze = spark.read.table(f"LH_test1.{nome_tabela}")
    
    # Regra genérica: Remove duplicados
    df_silver = df_bronze.dropDuplicates()
    
    # Gravação dinâmica com o mesmo nome na camada Silver
    df_silver.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"LH_Silver.{nome_tabela}")
        
    print(f"[SUCESSO] Tabela {nome_tabela} guardada no Silver!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
from pyspark.sql import functions as F

# ==========================================
# 1. LEITURA DOS DADOS (Camada Bronze)
# ==========================================
# Lemos a tabela que o seu Copy Job acabou de criar no Lakehouse Bronze
# (Ajuste o nome 'LH_test1' e 'nyc_taxi_green' se os seus nomes forem diferentes)
df_bronze = spark.read.table("LH_test1.nyc_taxi_green")

print(f"[INFO] Total de linhas lidas da camada Bronze: {df_bronze.count()}")

# ==========================================
# 2. LIMPEZA & DATA QUALITY (Camada Silver)
# ==========================================

# Passo A: Remover linhas completamente duplicadas
df_clean = df_bronze.dropDuplicates()

# Passo B: Remover linhas onde chaves ou datas essenciais sejam nulas (NULL)
df_clean = df_clean.dropna(subset=["VendorID", "lpep_pickup_datetime"])

# Passo C: Regras de Negócio / Verificação de Sanidade (Data Quality)
# No dataset de táxis, há muitas anomalias (erros de introdução de dados). Vamos filtrar:
# - Viagens têm de ter pelo menos 1 passageiro
# - A distância da viagem tem de ser maior que zero
# - O valor da tarifa não pode ser negativo
df_silver = df_clean.filter(
    (F.col("passenger_count") > 0) & 
    (F.col("trip_distance") > 0) & 
    (F.col("fare_amount") >= 0)
)

print(f"[INFO] Total de linhas limpas e validadas: {df_silver.count()}")

# ==========================================
# 3. GRAVAÇÃO DOS DADOS (Camada Silver)
# ==========================================
# Gravamos o resultado final no formato Delta dentro do Lakehouse Silver.
# O modo "overwrite" substitui a tabela se ela já existir (ótimo para testes).
df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("LH_Silver.nyc_taxi_clean")

print("[SUCESSO] Dados processados e guardados em LH_Silver.nyc_taxi_clean!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
