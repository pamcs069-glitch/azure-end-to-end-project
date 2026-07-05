# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_get_watermark
# MAGIC **NOT called by ADF.** This notebook is for manual testing only.
# MAGIC
# MAGIC In the v3 pipeline the Lookup Activity (`act_get_watermark`) reads the watermark directly
# MAGIC from the `pipeline_audit` Delta table via `ls_databricks_cluster` — no notebook is involved.
# MAGIC
# MAGIC **When to use this notebook:**
# MAGIC Run manually from Databricks to verify the audit table contents or test watermark logic
# MAGIC outside of ADF (e.g. to check what watermark the next incremental run will pick up).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1 — Parameters

dbutils.widgets.text("pipeline_name", "pl_bronze_api_payments_v3")
dbutils.widgets.text("load_type",     "incremental")

pipeline_name = dbutils.widgets.get("pipeline_name")
load_type     = dbutils.widgets.get("load_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2 — Read watermark from audit table

if load_type == "full":
    last_watermark = "1900-01-01T00:00:00Z"
    print(f"Full load — using epoch watermark: {last_watermark}")
else:
    result = spark.sql(f"""
        SELECT COALESCE(MAX(watermark_value), '1900-01-01T00:00:00Z') AS last_watermark
        FROM   dbw_ev_intelligence_dev.default.pipeline_audit
        WHERE  pipeline_name = '{pipeline_name}'
          AND  status        = 'succeeded'
    """)
    last_watermark = result.collect()[0]["last_watermark"]
    print(f"Incremental load — watermark from audit table: {last_watermark}")

print(f"last_watermark = {last_watermark}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3 — Show recent audit rows

display(
    spark.sql("""
        SELECT pipeline_name, load_type, watermark_value, ingestion_date,
               total_pages, status, run_timestamp
        FROM   dbw_ev_intelligence_dev.default.pipeline_audit
        ORDER  BY run_timestamp DESC
        LIMIT  10
    """)
)
