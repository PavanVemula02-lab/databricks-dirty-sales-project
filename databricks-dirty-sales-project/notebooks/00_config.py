catalog_name = "hive_metastore"

bronze_db = "bronze_sales"
silver_db = "silver_sales"
gold_db = "gold_sales"

input_path = "/FileStore/databricks_practice_sales_1M_dirty.xlsx"

bronze_table = f"{bronze_db}.sales_raw"
silver_table = f"{silver_db}.sales_clean"
gold_table = f"{gold_db}.sales_region_summary"
dq_table = f"{gold_db}.sales_dq_summary"
print(catalog_name, bronze_db, silver_db, gold_db, input_path, bronze_table, silver_table, gold_table, dq_table)