# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Superstore Pipeline Workflow - DataFrame ETL
# MAGIC
# MAGIC With growing demands and cut-throat competitions in the market, a Superstore Giant is seeking your knowledge in understanding what works best for them. They would like to understand which products, regions, categories and customer segments they should target or avoid.
# MAGIC
# MAGIC The goal of this analysis is to generate some insight from the data and answer some business questions. The question can like Top product in terms of sales, Which Region is most profitable etc.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Load Dataset as DataFrame
# MAGIC
# MAGIC This section loads a dataset as a DataFrame and examines a few rows of it to understand the schema.

# COMMAND ----------

import numpy as np
import pandas as pd

# File location and type
file_location = "/FileStore/tables/Sample___Superstore.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding of Dataset
# MAGIC
# MAGIC Let's examine the data to get a better understanding of what is there. We only examine a couple of features (columns).
# MAGIC
# MAGIC #### Meta Data
# MAGIC
# MAGIC Row ID => Unique ID for each row.
# MAGIC
# MAGIC Order ID => Unique Order ID for each Customer.
# MAGIC
# MAGIC Order Date => Order Date of the product.
# MAGIC
# MAGIC Ship Date => Shipping Date of the Product.
# MAGIC
# MAGIC Ship Mode=> Shipping Mode specified by the Customer.
# MAGIC
# MAGIC Customer ID => Unique ID to identify each Customer.
# MAGIC
# MAGIC Customer Name => Name of the Customer.
# MAGIC
# MAGIC Segment => The segment where the Customer belongs.
# MAGIC
# MAGIC Country => Country of residence of the Customer.
# MAGIC
# MAGIC City => City of residence of of the Customer.
# MAGIC
# MAGIC State => State of residence of the Customer.
# MAGIC
# MAGIC Postal Code => Postal Code of every Customer.
# MAGIC
# MAGIC Region => Region where the Customer belong.
# MAGIC
# MAGIC Product ID => Unique ID of the Product.
# MAGIC
# MAGIC Category => Category of the product ordered.
# MAGIC
# MAGIC Sub-Category => Sub-Category of the product ordered.
# MAGIC
# MAGIC Product Name => Name of the Product
# MAGIC
# MAGIC Sales => Sales of the Product.
# MAGIC
# MAGIC Quantity => Quantity of the Product.
# MAGIC
# MAGIC Discount => Discount provided.
# MAGIC
# MAGIC Profit => Profit/Loss incurred.

# COMMAND ----------

df.columns

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### View Data
# MAGIC View the unique categories in the data frame.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Products Name in the Dataset

# COMMAND ----------

products= df.select("product name").distinct()
display(products)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Display All States 

# COMMAND ----------

states=df.select("State").distinct()
display(states)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tamporary View
# MAGIC Here we are creating a Temporary View which is local database table that can be required as a normal View. **Temporary Views** are
# MAGIC handly when we want to customized view of data.

# COMMAND ----------

# Create a view or table

temp_table_name = "ss_table"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Queries
# MAGIC Here we use SQL quries to visualize the Data in the form of Graphs.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC SELECT discount, COUNT(discount) AS total
# MAGIC FROM ss_table
# MAGIC GROUP BY discount
# MAGIC ORDER BY discount

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC SELECT Segment, count(Segment) as total
# MAGIC FROM ss_table 
# MAGIC WHERE state='California'
# MAGIC GROUP BY Segment
# MAGIC ORDER BY Segment

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT Sales, sum(profit) as total_profit
# MAGIC FROM ss_table 
# MAGIC WHERE state='Florida'
# MAGIC GROUP BY Sales
# MAGIC ORDER BY Sales

# COMMAND ----------

# MAGIC %md
# MAGIC ####Porfit and Sales in cities of United States

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SALES, Profit, city FROM ss_table WHERE Country='United States'
# MAGIC ORDER BY city

# COMMAND ----------

# MAGIC %md
# MAGIC #### Region wise Revenue
# MAGIC Let’s start on the basis of the region we will see which region is having the highest revenue share. For this, we need two columns from the data namely Sales and Region. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Region, Sum(Sales) from ss_table 
# MAGIC GROUP BY Region
# MAGIC ORDER BY Region

# COMMAND ----------

# MAGIC %md
# MAGIC #### Segment Wise Revenue
# MAGIC The objective of this analysis is to find which segment is having the highest revenue share

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Segment, Sum(Sales) from ss_table 
# MAGIC GROUP BY Segment
# MAGIC ORDER BY Segment

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Category Wise Sales
# MAGIC The objective of this analysis is to find which ***product category is having the highest revenue share***.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, Sum(Sales) from ss_table 
# MAGIC GROUP BY category
# MAGIC ORDER BY category

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion and Insights
# MAGIC
# MAGIC From the above visualization, we can see that the West region is having the highest share in revenue with $725K whereas the South region is the lowest share in revenue with $392K
# MAGIC
# MAGIC From the above visualization, we can see that Consumer goods are having the highest share in revenue with $1161K whereas Home Office is the lowest share in revenue with $430K
# MAGIC
# MAGIC From the above visualization, we can see that Technological products is having the highest in revenue with 36.4% share worth $836K whereas Office Product is the lowest share in revenue with 31.3% share worth $430K.
