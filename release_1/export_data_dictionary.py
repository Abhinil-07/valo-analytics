# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Export Data Dictionary to Excel
# MAGIC 
# MAGIC Inspects all Bronze and Landing Delta tables in the `valorant` catalog, extracts their column names, data types, nullability, comments, and creates an Excel file with:
# MAGIC 1. An **Overview / Summary** sheet listing all tables, row counts, and column counts.
# MAGIC 2. A **separate sheet for each table** listing all columns, data types, and descriptions.
# MAGIC 3. An automatic HTML **Download Link** directly inside the notebook.

# COMMAND ----------
import os
import pandas as pd
import base64
from IPython.display import HTML, display

# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = "valorant"
SCHEMAS = ["bronze", "landing"]

# Local temp file in Databricks driver node
OUTPUT_FILENAME = "valorant_data_dictionary.xlsx"
LOCAL_OUTPUT_PATH = f"/tmp/{OUTPUT_FILENAME}"

# ==============================================================================
# STEP 1: DISCOVER TABLES
# ==============================================================================
all_tables = []

for schema in SCHEMAS:
    try:
        tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}")
        for row in tables_df.collect():
            table_name = row["tableName"]
            all_tables.append((schema, table_name))
    except Exception as e:
        print(f"Warning: could not inspect schema {schema}: {e}")

print(f"Discovered {len(all_tables)} table(s) across schemas: {SCHEMAS}")
for schema, tname in all_tables:
    print(f"  - {CATALOG}.{schema}.{tname}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 2: Extract Column Metadata and Build Excel Sheets

# COMMAND ----------
overview_rows = []
table_sheets = {}

with pd.ExcelWriter(LOCAL_OUTPUT_PATH, engine="openpyxl") as writer:
    for schema, table_name in all_tables:
        full_table_name = f"{CATALOG}.{schema}.{table_name}"
        
        try:
            # 1. Extract columns and data types via DESCRIBE TABLE
            desc_df = spark.sql(f"DESCRIBE TABLE {full_table_name}").collect()
            cols_data = []
            
            for r in desc_df:
                col_name = r["col_name"]
                # Skip partition or internal metadata divider headers
                if col_name.startswith("#") or col_name == "":
                    break
                cols_data.append({
                    "Column Name": col_name,
                    "Data Type": r["data_type"],
                    "Comment / Description": r["comment"] if "comment" in r and r["comment"] else ""
                })

            # 2. Get total row count
            try:
                row_cnt = spark.sql(f"SELECT COUNT(*) FROM {full_table_name}").collect()[0][0]
            except Exception:
                row_cnt = "N/A"

            # 3. Add to Overview
            overview_rows.append({
                "Schema": schema,
                "Table Name": table_name,
                "Column Count": len(cols_data),
                "Row Count": row_cnt,
                "Full Identifier": full_table_name
            })

            # 4. Write table-specific sheet (Excel sheet name max length is 31 chars)
            sheet_title = table_name[:31]
            df_table = pd.DataFrame(cols_data)
            df_table.to_excel(writer, sheet_name=sheet_title, index=False)
            print(f"Added sheet for {full_table_name} ({len(cols_data)} columns, {row_cnt} rows)")

        except Exception as err:
            print(f"Error inspecting {full_table_name}: {err}")

    # 5. Write the Overview sheet as the first sheet
    df_overview = pd.DataFrame(overview_rows)
    df_overview.to_excel(writer, sheet_name="Overview", index=False)

print(f"\nExcel workbook successfully written to: {LOCAL_OUTPUT_PATH}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Step 3: Download Link
# MAGIC 
# MAGIC Click the link below to download your generated Excel workbook directly to your machine!

# COMMAND ----------
with open(LOCAL_OUTPUT_PATH, "rb") as f:
    excel_bytes = f.read()

b64_data = base64.b64encode(excel_bytes).decode()
download_link = f"""
<div style="padding: 16px; background-color: #f0fdf4; border: 2px solid #22c55e; border-radius: 8px; margin: 10px 0;">
    <h3 style="margin-top: 0; color: #15803d;">Data Dictionary Ready!</h3>
    <p>Click below to download the Excel workbook containing column definitions and data types for all tables:</p>
    <a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_data}" 
       download="{OUTPUT_FILENAME}" 
       style="display: inline-block; padding: 10px 20px; background-color: #0284c7; color: white; text-decoration: none; font-weight: bold; border-radius: 6px;">
       Download {OUTPUT_FILENAME}
    </a>
</div>
"""

display(HTML(download_link))
