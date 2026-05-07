# Databricks notebook source

# MAGIC %md
# MAGIC # Shoppa Pipeline Challenge
# MAGIC
# MAGIC **Cenário:** Você entrou na equipe de dados da Shoppa, um marketplace com 8M de usuários.
# MAGIC O time de produto quer um dashboard de sessões por categoria — mas o pipeline de Spark
# MAGIC trava toda vez que algum produto viraliza no TikTok.
# MAGIC
# MAGIC **Seu trabalho:** entender por que isso acontece e consertar.
# MAGIC
# MAGIC > **Pré-requisito:** execute `setup.py` uma vez para gerar os dados.

# COMMAND ----------

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return (
            SparkSession.builder
            .appName("Shoppa")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )


def load_data(spark: SparkSession):
    """Carrega os dois datasets — Delta tables no Databricks, Parquet local."""
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        clicks   = spark.table("shoppa_clicks")
        products = spark.table("shoppa_products")
    else:
        clicks   = spark.read.parquet("data/clickstream")
        products = spark.read.parquet("data/products")
    return clicks, products


spark = get_spark()
clicks, products = load_data(spark)

print("Datasets carregados. Pode começar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 — Perfil dos dados `[20 pts]`
# MAGIC
# MAGIC Usando os DataFrames `clicks` e `products`, responda:
# MAGIC
# MAGIC 1. Quantas linhas tem cada dataset?
# MAGIC 2. Qual é a distribuição de `event_type`?
# MAGIC 3. Qual produto tem mais eventos? Qual % do total ele representa?
# MAGIC 4. Imprima o top 10 produtos por volume de eventos.
# MAGIC
# MAGIC **Entregável:** código + prints dos resultados.
# MAGIC
# MAGIC **Pergunta para o `ANALYSIS.md`:**
# MAGIC O que você observa na distribuição de `product_id`? Isso sugere algum problema quando você fizer um join?

# COMMAND ----------

# Task 1 — SEU CÓDIGO AQUI ↓

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2 — Join naive e diagnóstico `[20 pts]`
# MAGIC
# MAGIC Escreva o join mais simples possível entre `clicks` e `products`.
# MAGIC Calcule por categoria e por dia:
# MAGIC - Total de sessões distintas
# MAGIC - Receita total
# MAGIC - Taxa de conversão (purchases / views)
# MAGIC
# MAGIC ```
# MAGIC +------------+----------+--------+---------+----------------+
# MAGIC | categoria  |   data   | sessoes| receita |taxa_conversao  |
# MAGIC +------------+----------+--------+---------+----------------+
# MAGIC ```
# MAGIC
# MAGIC **Entregável:** código + análise de 3–5 linhas no `ANALYSIS.md`:
# MAGIC 1. Quanto tempo demorou?
# MAGIC 2. Qual stage está lento? (abra o Spark UI)
# MAGIC 3. Como você confirmaria que é o join o culpado e não outra coisa?

# COMMAND ----------

# Task 2 — SEU CÓDIGO AQUI ↓

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3 — Fix com Broadcast Join `[20 pts]`
# MAGIC
# MAGIC O catálogo de produtos tem 10.000 linhas — cabe facilmente em memória de cada executor.
# MAGIC
# MAGIC 1. Reescreva o join usando broadcast join
# MAGIC 2. Meça o tempo novamente e compare com a Task 2
# MAGIC 3. Verifique o plano de execução com `.explain()`
# MAGIC
# MAGIC **Entregável:** código + no `ANALYSIS.md`:
# MAGIC - Por que o broadcast join elimina o problema neste caso?
# MAGIC - Qual regra de bolso você usaria para decidir entre broadcast e sort-merge join em produção?

# COMMAND ----------

# Task 3 — SEU CÓDIGO AQUI ↓

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 — Fix com Salting `[25 pts]`
# MAGIC
# MAGIC Imagine que o catálogo cresceu para 50 milhões de produtos — broadcast não é mais uma opção.
# MAGIC
# MAGIC Implemente salting para resolver o problema no sort-merge join:
# MAGIC 1. Adicione coluna `salt` (inteiro aleatório `0..N-1`) no `clicks`
# MAGIC 2. Exploda o dataset de produtos: uma linha por valor de salt
# MAGIC 3. Faça o join em `(product_id, salt)`
# MAGIC 4. Agregue normalmente
# MAGIC
# MAGIC **Entregável:** código + no `ANALYSIS.md`:
# MAGIC - Qual o valor ideal de N? Como você escolheria em produção?
# MAGIC - Qual o tradeoff de aumentar muito N?

# COMMAND ----------

# Task 4 — SEU CÓDIGO AQUI ↓

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 — Output e Teste Unitário `[15 pts]`
# MAGIC
# MAGIC **Parte 1:** salve o resultado final como Delta table particionada por `data`
# MAGIC (Parquet se não tiver Delta disponível).
# MAGIC
# MAGIC **Parte 2:** escreva 1 teste unitário para a função de taxa de conversão:
# MAGIC - Use `spark.createDataFrame()` com dados criados na mão
# MAGIC - Valide schema + pelo menos 1 valor calculado
# MAGIC - Cubra pelo menos 1 edge case (ex: categoria sem views)
# MAGIC - Use `pytest` ou `unittest`
# MAGIC
# MAGIC > **Dica:** extraia a lógica de cálculo para uma função separada antes de testar —
# MAGIC > funções puras são muito mais fáceis de testar.

# COMMAND ----------

# Task 5 — SEU CÓDIGO AQUI ↓
