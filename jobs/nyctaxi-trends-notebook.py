# Databricks notebook source
# Show all catalogs in the metastore.
display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG samples")

# COMMAND ----------

# Shows schemas in current catalog
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Set the current schema.
spark.sql("USE nyctaxi")

# COMMAND ----------

#Show current tables in schema
display(spark.sql("SHOW TABLES"))

# COMMAND ----------

#Show table
display(spark.table("trips"))

# COMMAND ----------

# Read a table into a DataFrame and display
df = spark.read.table("samples.nyctaxi.trips")
display(df)

# COMMAND ----------

# Create a catalog.
spark.sql("CREATE CATALOG IF NOT EXISTS trends")


# COMMAND ----------

# Change into new catalog
spark.sql("USE CATALOG trends")

# COMMAND ----------

# Grant create and usage permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE, USAGE
  ON CATALOG trends
  TO `account users`""")

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS nyctaxi
  COMMENT 'A new Unity Catalog schema called nyctaxi'""")

# COMMAND ----------

# Grant create and usage permissions for the schema to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE, USAGE
  ON SCHEMA nyctaxi
  TO `account users`""")

# COMMAND ----------

# Set the current schema.
spark.sql("USE nyctaxi")

# COMMAND ----------

# Save previous dataframe as a table in new catalog.schema
df.write.mode("overwrite").saveAsTable("trips_analysis_oct_2022")

# COMMAND ----------

# Grant select and modify permissions for the table to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT SELECT, MODIFY
  ON TABLE trips_analysis_oct_2022
  TO `account users`""")

# COMMAND ----------

# Show results from new catalog, schema, table
display(spark.read.table("trends.nyctaxi.trips_analysis_oct_2022"))

# COMMAND ----------


