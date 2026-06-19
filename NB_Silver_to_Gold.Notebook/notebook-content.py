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
# META           "id": "2fbffbcb-0e91-4220-b419-6594ca6a6fd0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# ====================================================================
# 1. ENCONTRAR TODAS AS TABELAS NA CAMADA SILVER
# ====================================================================
lista_tabelas = spark.catalog.listTables("LH_Silver")

#alteracao

for tabela in lista_tabelas:
    nome_tabela = tabela.name
    
    # Ignorar tabelas de sistema ou lixo se existirem
    if nome_tabela == "dbo":
        continue
        
    print(f"[LEITURA] A ler a tabela limpa do Silver: {nome_tabela}")
    df_silver = spark.read.table(f"LH_Silver.{nome_tabela}")
    
    # ====================================================================
    # 2. APLICAR REGRAS DE NEGÓCIO SEGUNDO O NOME DA TABELA (Camada Gold)
    # ====================================================================
    
    if nome_tabela == "diabetes":
        print(f"[MÉTRICAS] A calcular médias e agregações para {nome_tabela}...")
        # Exemplo Gold: Agrupar por uma coluna (ex: Resultado/Outcome) e calcular médias das taxas
        # (Substitua "Outcome" e "Glucose" pelos nomes reais das colunas da sua tabela)
        df_gold = df_silver.groupBy("Outcome").agg(
            F.count("Outcome").alias("total_pacientes"),
            F.round(F.avg("Glucose"), 2).alias("media_glicose")
        )
        nome_saida_gold = "fact_diabetes_summary"

    elif nome_tabela == "holidays":
        print(f"[MODELAÇÃO] A otimizar a tabela de feriados para o Power BI...")
        # Exemplo Gold: Criar colunas de calendário úteis para os relatórios
        # (Substitua "date" pela coluna real de data que tiver nos feriados)
        df_gold = df_silver.withColumn("ano_feriado", F.year("date")) \
                           .withColumn("mes_feriado", F.month("date"))
        nome_saida_gold = "dim_holidays"

    else:
        # Se aparecer uma tabela nova que ainda não tem regra de negócio especial, 
        # o Spark apenas a passa para a frente para não quebrar a pipeline
        print(f"[AVISO] Tabela {nome_tabela} sem regra Gold específica. A passar cópia direta...")
        df_gold = df_silver
        nome_saida_gold = f"gold_{nome_tabela}"

    # ====================================================================
    # 3. GRAVAR O RESULTADO AGREGADO NO LAKEHOUSE GOLD
    # ====================================================================
    df_gold.write \
        .mode("overwrite") \
        .format("delta") \
        .saveAsTable(f"LH_Gold.{nome_saida_gold}")
        
    print(f"[SUCESSO] Tabela Gold '{nome_saida_gold}' gerada com sucesso!\n")

print("[PROCESSO CONCLUÍDO] Todos os cubos e dimensões foram gerados no LH_Gold!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
from pyspark.sql import functions as F

# ==========================================
# 1. LEITURA DOS DADOS (Camada Silver)
# ==========================================
df_silver = spark.read.table("LH_Silver.nyc_taxi_clean")

# ==========================================
# 2. AGREGAÇÃO DE NEGÓCIO (Criação de Métricas)
# ==========================================
# Vamos extrair apenas a DATA (sem a hora) para agrupar as métricas por dia
df_gold = df_silver.withColumn("pickup_date", F.to_date("lpep_pickup_datetime"))

# Agrupamos por Data e por Fornecedor (VendorID) para calcular os totais
df_metrics = df_gold.groupBy("pickup_date", "VendorID").agg(
    F.count("VendorID").alias("total_viagens"),
    F.sum("passenger_count").alias("total_passageiros"),
    F.sum("trip_distance").alias("distancia_total_km"),
    F.sum("fare_amount").alias("faturado_tarifas"),
    F.round(F.avg("tip_amount"), 2).alias("media_gorjetas")
).orderBy("pickup_date")

# ==========================================
# 3. GRAVAÇÃO DOS DADOS (Camada Gold)
# ==========================================
df_metrics.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("LH_Gold.daily_taxi_summary")

print("[SUCESSO] Cubo de dados Gold gerado em LH_Gold.daily_taxi_summary!")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
