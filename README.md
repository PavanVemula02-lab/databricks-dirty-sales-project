Dataset details:

```text
Project name: databricks_practice_sales_1M_dirty
Sheet 1: sales_dirty_1M
Sheet 2: data_dictionary
Rows: 1,000,000 data rows + header
Columns: 6
```

Columns:

```text
order_id
order_date
region
quantity
unit_price
email
```

Dirty data included:

```text
Null values
N/A values
Negative numbers
Invalid dates
Different date formats
String values inside numeric columns
Invalid email formats
Blank regions
UNKNOWN / N/A region values
```

***

# Project Scenario

## Project Name

```text
Databricks Dirty Sales Data Pipeline
```

## Business Scenario

A sales team receives daily order data in Excel format. The file is very large and contains dirty data. Your job as a data engineer is to build a Databricks pipeline that:

1. Reads Excel data.
2. Stores raw data in a Bronze table.
3. Cleans and validates data into a Silver table.
4. Creates business summary data in a Gold table.
5. Runs data quality checks.
6. Uses `%run` for common configuration.
7. Uses `dbutils.notebook.run()` for notebook workflow practice.
8. Creates Databricks Jobs with task dependencies.
9. Creates one job that depends on another job.
10. Uses Git repo and Databricks Workspace Repos / Git folders.

Databricks recommends Jobs / Lakeflow Jobs for notebook orchestration because they support task dependencies, scheduling, triggers, and complex workflows. [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/notebooks/notebook-workflows), [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/jobs/)

***

# Git Repo Structure

Create a GitHub or Azure DevOps repo with this structure:

```text
databricks-dirty-sales-project/
│
├── notebooks/
│   ├── 00_config.py
│   ├── 01_ingest_bronze.py
│   ├── 02_clean_silver.py
│   ├── 03_gold_aggregations.py
│   ├── 04_data_quality_checks.py
│   ├── 05_notebook_workflow_driver.py
│   └── 06_final_report.py
│
├── jobs/
│   └── job_dependency_design.md
│
├── data/
│   └── README.md
│
└── README.md
```

***

# Databricks Workspace Repo Setup

## Step 1: Create Git repo

Create repo:

```text
databricks-dirty-sales-project
```

Push your notebooks into the repo.

***

## Step 2: Connect Git in Databricks

In Databricks:

```text
Workspace → Repos / Git folders → Add Repo
```

Paste your Git URL.

Databricks lets you configure notebook tasks using workspace notebooks or notebooks from a Git-backed source. For production/scheduled jobs, Databricks recommends using Git provider integration for version-controlled job assets. [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/jobs/notebook), [\[learn.microsoft.com\]](https://learn.microsoft.com/en-us/azure/databricks/jobs/notebook)

***

## Step 3: Example Workspace Repo Path

Your repo path may look like:

```text
/Workspace/Repos/your_email@databricks.com/databricks-dirty-sales-project
```

Inside the repo:

```text
/Workspace/Repos/your_email@databricks.com/databricks-dirty-sales-project/notebooks
```

***

# Upload Excel File to Databricks

Upload the Excel file to:

```text
/FileStore/databricks_practice_sales_1M_dirty.xlsx
```

Or, if you are using Unity Catalog Volumes:

```text
/Volumes/dev/default/raw/databricks_practice_sales_1M_dirty.xlsx
```

For beginner practice, use:

```text
/FileStore/databricks_practice_sales_1M_dirty.xlsx
```

***

# Notebook 1: `00_config.py`

Purpose: Store common variables.

Use this notebook with `%run` in all other notebooks.

```python
catalog_name = "hive_metastore"

bronze_db = "bronze_sales"
silver_db = "silver_sales"
gold_db = "gold_sales"

input_path = "/FileStore/databricks_practice_sales_1M_dirty.xlsx"

bronze_table = f"{bronze_db}.sales_raw"
silver_table = f"{silver_db}.sales_clean"
gold_table = f"{gold_db}.sales_region_summary"
dq_table = f"{gold_db}.sales_dq_summary"
```

***

# Notebook 2: `01_ingest_bronze.py`

Purpose: Read Excel data and create Bronze table.

Use `%run`:

```python
%run ./00_config
```

Code:

```python
spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")

df = (
    spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("dataAddress", "'sales_dirty_1M'!A1")
    .load(input_path)
)

df.write.mode("overwrite").saveAsTable(bronze_table)

print("Bronze load completed")
print("Bronze row count:", df.count())
```

The `%run` command includes another notebook inline, making its variables and functions available in the calling notebook. It is useful for modularizing shared notebook code. [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/notebooks/notebook-workflows), [\[docs.azure.cn\]](https://docs.azure.cn/en-us/databricks/notebooks/notebook-workflows)

***

# Notebook 3: `02_clean_silver.py`

Purpose: Clean nulls, datatype mismatches, invalid dates, negative values, and bad emails.

```python
%run ./00_config
```

Code:

```python
from pyspark.sql.functions import col, trim, lower, when, to_date

spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

df = spark.table(bronze_table)

clean_df = (
    df
    .withColumn("order_id_clean", col("order_id").cast("int"))
    .withColumn(
        "order_date_clean",
        when(
            to_date(col("order_date"), "yyyy-MM-dd").isNotNull(),
            to_date(col("order_date"), "yyyy-MM-dd")
        )
        .when(
            to_date(col("order_date"), "dd-MM-yyyy").isNotNull(),
            to_date(col("order_date"), "dd-MM-yyyy")
        )
    )
    .withColumn(
        "region_clean",
        when(
            trim(lower(col("region"))).isin("", "unknown", "n/a"),
            None
        ).otherwise(trim(lower(col("region"))))
    )
    .withColumn("quantity_clean", col("quantity").cast("int"))
    .withColumn("unit_price_clean", col("unit_price").cast("double"))
    .withColumn(
        "email_clean",
        when(
            col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
            col("email")
        ).otherwise(None)
    )
)

clean_df = clean_df.filter(
    (col("order_id_clean").isNotNull()) &
    (col("order_id_clean") > 0) &
    (col("order_date_clean").isNotNull()) &
    (col("quantity_clean").isNotNull()) &
    (col("quantity_clean") > 0) &
    (col("unit_price_clean").isNotNull()) &
    (col("unit_price_clean") > 0)
)

final_df = clean_df.select(
    "order_id_clean",
    "order_date_clean",
    "region_clean",
    "quantity_clean",
    "unit_price_clean",
    "email_clean"
)

final_df.write.mode("overwrite").saveAsTable(silver_table)

print("Silver cleaning completed")
print("Silver row count:", final_df.count())
```

***

# Notebook 4: `03_gold_aggregations.py`

Purpose: Create region-wise sales summary.

```python
%run ./00_config
```

Code:

```python
from pyspark.sql.functions import col, count, sum, round

spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

df = spark.table(silver_table)

gold_df = (
    df
    .groupBy("region_clean")
    .agg(
        count("*").alias("total_orders"),
        sum("quantity_clean").alias("total_quantity"),
        round(sum(col("quantity_clean") * col("unit_price_clean")), 2).alias("total_sales")
    )
    .orderBy("region_clean")
)

gold_df.write.mode("overwrite").saveAsTable(gold_table)

display(gold_df)

print("Gold aggregation completed")
```

***

# Notebook 5: `04_data_quality_checks.py`

Purpose: Validate Silver and Gold tables.

```python
%run ./00_config
```

Code:

```python
from pyspark.sql.functions import col, count, when, lit

silver_df = spark.table(silver_table)
gold_df = spark.table(gold_table)

silver_count = silver_df.count()
gold_count = gold_df.count()

print("Silver count:", silver_count)
print("Gold count:", gold_count)

if silver_count == 0:
    raise Exception("DQ failed: Silver table has zero records")

if gold_count == 0:
    raise Exception("DQ failed: Gold table has zero records")

null_check_df = silver_df.select([
    count(when(col(c).isNull(), c)).alias(c + "_null_count")
    for c in silver_df.columns
])

display(null_check_df)

dq_summary = spark.createDataFrame(
    [
        ("silver_row_count", silver_count),
        ("gold_row_count", gold_count)
    ],
    ["metric_name", "metric_value"]
)

dq_summary.write.mode("overwrite").saveAsTable(dq_table)

print("Data quality checks completed")
```

***

# Notebook 6: `05_notebook_workflow_driver.py`

Purpose: Practice notebook workflows using `dbutils.notebook.run()`.

```python
result_1 = dbutils.notebook.run("./01_ingest_bronze", 0)
result_2 = dbutils.notebook.run("./02_clean_silver", 0)
result_3 = dbutils.notebook.run("./03_gold_aggregations", 0)
result_4 = dbutils.notebook.run("./04_data_quality_checks", 0)

print("Notebook workflow completed")
```

`dbutils.notebook.run()` is useful when you want notebook orchestration with parameters or return values, while `%run` is better for shared notebook code and simple modularization. [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/notebooks/notebook-workflows), [\[docs.azure.cn\]](https://docs.azure.cn/en-us/databricks/notebooks/notebook-workflows)

***

# Notebook 7: `06_final_report.py`

Purpose: Final reporting notebook.

```python
%run ./00_config
```

Code:

```python
gold_df = spark.table(gold_table)
dq_df = spark.table(dq_table)

print("Final Gold Summary")
display(gold_df)

print("DQ Summary")
display(dq_df)
```

***

# Task Dependency on Task

Create one Databricks job:

```text
JOB_01_Dirty_Sales_ETL
```

Inside this job, create these tasks:

```text
Task 1: ingest_bronze
Notebook: 01_ingest_bronze.py
Depends on: None

Task 2: clean_silver
Notebook: 02_clean_silver.py
Depends on: ingest_bronze

Task 3: gold_aggregations
Notebook: 03_gold_aggregations.py
Depends on: clean_silver

Task 4: data_quality_checks
Notebook: 04_data_quality_checks.py
Depends on: gold_aggregations
```

Databricks Jobs consist of one or more tasks, and tasks can be represented as a Directed Acyclic Graph, meaning one task can depend on another task. [\[docs.databricks.com\]](https://docs.databricks.com/aws/en/jobs/)

## Task Dependency Diagram

```text
JOB_01_Dirty_Sales_ETL

ingest_bronze
      |
      v
clean_silver
      |
      v
gold_aggregations
      |
      v
data_quality_checks
```

This is called:

```text
Task dependency on task
```

Example:

```text
clean_silver depends on ingest_bronze
gold_aggregations depends on clean_silver
data_quality_checks depends on gold_aggregations
```

***

# Job Dependency on Job

Now create a second Databricks job:

```text
JOB_02_Final_Sales_Report
```

This job should depend on the first job.

Use a **Run Job task**.

Databricks supports a `Run Job` task, which is used to trigger another job in the same Databricks workspace. Circular job dependencies are not supported. [\[learn.microsoft.com\]](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-job)

***

## JOB\_02 Task Design

```text
Task 1: run_sales_etl_job
Type: Run Job
Job to run: JOB_01_Dirty_Sales_ETL
Depends on: None

Task 2: final_report
Type: Notebook
Notebook: 06_final_report.py
Depends on: run_sales_etl_job
```

## Job Dependency Diagram

```text
JOB_02_Final_Sales_Report

run_sales_etl_job
      |
      v
final_report
```

Where:

```text
run_sales_etl_job triggers JOB_01_Dirty_Sales_ETL
```

Full dependency:

```text
JOB_02_Final_Sales_Report
        |
        v
Run Job Task: JOB_01_Dirty_Sales_ETL
        |
        v
06_final_report.py
```

***

# Job Dependency on Task

You specifically asked for **job dependency on task**.

In Databricks, a job does not directly depend on a single task from another job. Instead, you usually design it like this:

```text
JOB_02 has a Run Job task
        |
        v
The Run Job task triggers JOB_01
        |
        v
After JOB_01 finishes successfully, next task in JOB_02 runs
```

So practically:

```text
Task final_report in JOB_02 depends on task run_sales_etl_job
```

And:

```text
run_sales_etl_job represents dependency on JOB_01_Dirty_Sales_ETL
```

Correct design:

```text
JOB_02_Final_Sales_Report
    Task 1: run_sales_etl_job
        Runs JOB_01_Dirty_Sales_ETL

    Task 2: final_report
        Depends on Task 1
```

So the dependency is implemented as:

```text
Job dependency through a Run Job task
```

***

# Complete Dependency Architecture

```text
JOB_01_Dirty_Sales_ETL

01_ingest_bronze
      |
      v
02_clean_silver
      |
      v
03_gold_aggregations
      |
      v
04_data_quality_checks


JOB_02_Final_Sales_Report

run_sales_etl_job
      |
      v
06_final_report
```

And internally:

```text
JOB_02.run_sales_etl_job
        |
        v
JOB_01_Dirty_Sales_ETL
        |
        v
JOB_02.final_report
```

***

# What You Need to Practice

## Part 1: Git

Practice these steps:

```text
1. Create Git repo.
2. Create notebook folder.
3. Add all notebooks.
4. Commit code.
5. Push to Git.
6. Connect repo in Databricks Workspace Repos.
7. Pull latest code.
8. Make one code change.
9. Commit and push from Databricks.
```

***

## Part 2: Databricks Notebooks

Practice:

```text
1. Upload Excel file.
2. Create 00_config notebook.
3. Use %run in all notebooks.
4. Read Excel file.
5. Create Bronze table.
6. Clean data into Silver table.
7. Aggregate data into Gold table.
8. Run DQ checks.
9. Run notebook driver using dbutils.notebook.run().
```

***

## Part 3: Databricks Jobs

Practice:

```text
1. Create JOB_01_Dirty_Sales_ETL.
2. Add 4 notebook tasks.
3. Add task dependency on task.
4. Run the job.
5. Validate Bronze, Silver, and Gold tables.
6. Create JOB_02_Final_Sales_Report.
7. Add Run Job task to trigger JOB_01.
8. Add final report task.
9. Add task dependency from final_report to run_sales_etl_job.
10. Run JOB_02.
```

***

# Expected Final Output

After completing the project, you should have these tables:

```text
bronze_sales.sales_raw
silver_sales.sales_clean
gold_sales.sales_region_summary
gold_sales.sales_dq_summary
```

Expected output from Gold table:

```text
region_clean | total_orders | total_quantity | total_sales
east         | ...          | ...            | ...
north        | ...          | ...            | ...
south        | ...          | ...            | ...
west         | ...          | ...            | ...
central      | ...          | ...            | ...
```

***

# Submission Checklist for Me to Validate

Once you complete the project, send me:

```text
1. Git repo folder structure
2. Databricks workspace repo path
3. Screenshot or list of notebooks
4. Bronze table row count
5. Silver table row count
6. Gold table output
7. DQ check output
8. JOB_01 task dependency design
9. JOB_02 job dependency design
10. Errors faced, if any
11. Your cleaning logic
12. Final observations
```

