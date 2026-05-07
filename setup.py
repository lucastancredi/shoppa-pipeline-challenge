# Databricks notebook source

# MAGIC %md
# MAGIC # Shoppa Pipeline Challenge — Setup
# MAGIC
# MAGIC Execute este notebook **uma vez** para gerar os datasets do challenge.
# MAGIC Depois, abra `challenge.py`.
# MAGIC
# MAGIC > Ambiente detectado automaticamente — sem configuração necessária.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Como importar no Databricks Community Edition
# MAGIC
# MAGIC 1. Acesse [community.cloud.databricks.com](https://community.cloud.databricks.com)
# MAGIC 2. No menu lateral: **Workspace → sua pasta pessoal**
# MAGIC 3. Clique em **Import** (ícone de seta para baixo ou botão no canto superior direito)
# MAGIC 4. Selecione **File** e faça upload de `setup.py`
# MAGIC 5. O Databricks abre o notebook automaticamente — clique em **Run All**
# MAGIC 6. Repita os passos 3–4 para `challenge.py`
# MAGIC
# MAGIC > **Atenção:** rode o `setup.py` antes de abrir o `challenge.py`.

# COMMAND ----------

import os
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

_ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
N_EVENTS       = 2_000_000 if _ON_DATABRICKS else 500_000
TABLE_CLICKS   = "shoppa_clicks"
TABLE_PRODUCTS = "shoppa_products"
LOCAL_DIR      = "data"

_CATEGORIES   = ["Eletrônicos", "Moda", "Casa", "Esportes", "Beleza", "Livros", "Games"]
_NUM_PRODUCTS = 10_000
_NUM_USERS    = 200_000


def _get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return (
            SparkSession.builder
            .appName("Shoppa-Setup")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )


def _data_exists(spark: SparkSession) -> bool:
    if _ON_DATABRICKS:
        return spark.catalog.tableExists(TABLE_CLICKS)
    return os.path.exists(f"{LOCAL_DIR}/clickstream")


def _build_products(rng) -> pd.DataFrame:
    return pd.DataFrame({
        "product_id":   [f"P{i:06d}" for i in range(_NUM_PRODUCTS)],
        "product_name": [f"Produto {i}" for i in range(_NUM_PRODUCTS)],
        "category":     rng.choice(_CATEGORIES, _NUM_PRODUCTS),
        "price":        np.round(rng.uniform(10.0, 2000.0, _NUM_PRODUCTS), 2),
        "seller_id":    [f"S{v:04d}" for v in rng.integers(0, 500, _NUM_PRODUCTS)],
    })


def _build_clicks(rng, n_events: int) -> pd.DataFrame:
    ranks = np.arange(1, _NUM_PRODUCTS + 1)
    w = 1.0 / (ranks ** 1.8)
    w[:5] *= 50
    w /= w.sum()

    product_indices = rng.choice(_NUM_PRODUCTS, n_events, p=w)
    seconds_offset  = rng.integers(0, 30 * 24 * 3600, n_events)
    timestamps      = pd.Timestamp("2024-01-01") + pd.to_timedelta(seconds_offset, unit="s")
    event_types     = rng.choice(["view", "add_to_cart", "purchase"], n_events, p=[0.50, 0.33, 0.17])
    revenues        = np.where(event_types == "purchase",
                               np.round(rng.uniform(10.0, 2000.0, n_events), 2), np.nan)
    return pd.DataFrame({
        "event_id":   [f"E{i:09d}" for i in range(n_events)],
        "session_id": [f"S{v:08d}" for v in rng.integers(0, n_events // 5, n_events)],
        "user_id":    [f"U{v:07d}" for v in rng.integers(0, _NUM_USERS, n_events)],
        "product_id": [f"P{idx:06d}" for idx in product_indices],
        "event_type": event_types,
        "event_ts":   timestamps,
        "revenue":    revenues,
    })

# COMMAND ----------

def generate(n_events: int = N_EVENTS) -> None:
    spark = _get_spark()

    if _data_exists(spark):
        print("Dataset já existe. Delete as tabelas/pasta para regenerar.")
        return

    print(f"Gerando dataset ({n_events:,} eventos)... aguarde.")
    rng = np.random.default_rng(42)

    products_df = spark.createDataFrame(_build_products(rng))
    clicks_df   = spark.createDataFrame(_build_clicks(rng, n_events))

    if _ON_DATABRICKS:
        products_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_PRODUCTS)
        clicks_df.write.format("delta").mode("overwrite").saveAsTable(TABLE_CLICKS)
        print(f"  → tabelas '{TABLE_PRODUCTS}' e '{TABLE_CLICKS}' criadas no catálogo")
    else:
        os.makedirs(LOCAL_DIR, exist_ok=True)
        products_df.write.mode("overwrite").parquet(f"{LOCAL_DIR}/products")
        clicks_df.write.mode("overwrite").parquet(f"{LOCAL_DIR}/clickstream")
        print(f"  → dados salvos em {LOCAL_DIR}/")

    print("\nPronto. Agora abra challenge.py e comece a Task 1.")


generate()
