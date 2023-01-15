# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Script to Transform Raw Data from Zillow API
# MAGIC 
# MAGIC The following steps will be taken:
# MAGIC 
# MAGIC 1. Read in raw data from Azure Data Lake Storage
# MAGIC 2. Process this data using spark dataframe
# MAGIC 3. Save data in tabular format
# MAGIC 4. Move raw file to a "processed" folder/container

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# display(dbutils.fs.mounts())
display(dbutils.fs.ls('/mnt/Files/ADLS/raw'))

# COMMAND ----------

df = spark.read.format("json").option("multiline","true").load("/mnt/Files/ADLS/raw")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

#df.columns

# COMMAND ----------

# %sql
# create schema data_analytics

# COMMAND ----------

# the data set has some nested json. I will unnest those columns
df = df.select('address',
 'bathrooms',
 'bedrooms',
 'contingentListingType',
 'country',
 'currency',
 'dateSold',
 'daysOnZillow',
 'hasImage',
 'imgSrc',
 'latitude',
 'listingStatus',
 'listingSubType.*',#unnest json within
 'livingArea',
 'longitude',
 'lotAreaUnit',
 'lotAreaValue',
 'price',
 'priceChange',
 'propertyType',
 'rentZestimate',
 'unit',
 'variableData.data.*',#unnest json within
    'variableData.text',
    'variableData.type',
 'zestimate',
 'zpid')

display(df)

# COMMAND ----------

# save to delta table

df.write.mode('append').saveAsTable('data_analytics.property_list')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from data_analytics.property_list

# COMMAND ----------

#move contents of 'raw' folder to 'processed' folder
dbutils.fs.mv("/mnt/Files/ADLS/raw","/mnt/Files/ADLS/processed",True)
