# Databricks notebook source
client ID : e47e4fc2-f5dd-4139-8138-6b6660cbe1c9
Directory (tenant) ID : 05af7fc5-5a44-41d1-af78-3f2ca0e2514c
8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9



# COMMAND ----------

#import libraries
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

#mounting databricks on top of data lake
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "e47e4fc2-f5dd-4139-8138-6b6660cbe1c9",
  "fs.azure.account.oauth2.client.secret": "8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05af7fc5-5a44-41d1-af78-3f2ca0e2514c/oauth2/token"
}

mounted_paths = [mount.mountPoint for mount in dbutils.fs.mounts()]

if mount_point not in mounted_paths:
    dbutils.fs.mount(
        source="abfss://raw-data@dataengineeringk.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
else:
    print(f"{mount_point} is already mounted.")

# COMMAND ----------

dim_customers_raw = spark.read.format("csv").option("header", "true").load("/mnt/dataengineering/dim_customers_raw")


# COMMAND ----------

dim_customers_raw.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, desc, regexp_replace, trim
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType


# COMMAND ----------

#CUSTOMERS
dim_customers_raw = spark.read.format("csv").option("header", "true").load("/mnt/dataengineering/dim_customers_raw")

dim_customers_raw = dim_customers_raw.withColumn("Type" , regexp_replace(col("Type"), "Type-", ""))

dim_customers_raw = dim_customers_raw.dropDuplicates(["Customer ID"])

dim_customers_raw = dim_customers_raw.orderBy("Customer ID")

dim_customers_raw = dim_customers_raw.filter(col("Age").cast("int").isNotNull())

dim_customers_raw = dim_customers_raw.withColumn("Gender", when(col("Gender") == "M", "Male").when(col("Gender") == "F", "Female").otherwise(col("Gender")))

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

#mounting databricks on top of data lake
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "e47e4fc2-f5dd-4139-8138-6b6660cbe1c9",
  "fs.azure.account.oauth2.client.secret": "8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05af7fc5-5a44-41d1-af78-3f2ca0e2514c/oauth2/token"
}
dbutils.fs.unmount("/mnt/dim_customers_raw")
dbutils.fs.mount(
      source="abfss://transformed-data@dataengineeringk.dfs.core.windows.net",
      mount_point="/mnt/dim_customers_raw",
      extra_configs=configs)

      
dim_customers_raw.write.option("header","true").csv("/mnt/dim_customers_raw/dim_customers_transformed")

# COMMAND ----------

dim_customers_raw.show()

# COMMAND ----------

dim_customers_raw.select("Age").show()

# COMMAND ----------

#Sales Channel
dim_sales_channel_raw = spark.read.format("csv").option("header", "true").load("/mnt/dataengineering/dim_sales_channel_raw")

dim_sales_channel_raw = dim_sales_channel_raw.na.drop()
dim_sales_channel_raw = dim_sales_channel_raw.dropDuplicates(["Sales Channel ID"])

dim_sales_channel_raw = dim_sales_channel_raw.orderBy("Sales Channel ID")

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "e47e4fc2-f5dd-4139-8138-6b6660cbe1c9",
  "fs.azure.account.oauth2.client.secret": "8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05af7fc5-5a44-41d1-af78-3f2ca0e2514c/oauth2/token"
}

dbutils.fs.mount(
      source="abfss://transformed-data@dataengineeringk.dfs.core.windows.net",
      mount_point="/mnt/dim_sales_channel_raw",
      extra_configs=configs)

      
dim_customers_raw.write.option("header","true").csv("/mnt/dim_sales_channel_raw/dim_sales_channel_transformed")

# COMMAND ----------

dim_payments_method_raw.show()

# COMMAND ----------

dim_payments_method_raw = spark.read.format("csv").option("header", "true").load("/mnt/dataengineering/dim_payments_method_raw")
dim_payments_method_raw = dim_payments_method_raw.na.drop()

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "e47e4fc2-f5dd-4139-8138-6b6660cbe1c9",
  "fs.azure.account.oauth2.client.secret": "8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05af7fc5-5a44-41d1-af78-3f2ca0e2514c/oauth2/token"
}
dbutils.fs.unmount("/mnt/dim_payments_method_raw")
dbutils.fs.mount(
      source="abfss://transformed-data@dataengineeringk.dfs.core.windows.net",
      mount_point="/mnt/dim_payments_method_raw",
      extra_configs=configs)

      
dim_payments_method_raw.write.option("header", "true").csv("/mnt/dim_payments_method_raw/dim_payments_method_transformed")

# COMMAND ----------

fact_sales_raw = spark.read.format("csv").option("header", "true").load("/mnt/dataengineering/fact_sales_raw")

fact_sales_raw = fact_sales_raw.drop("Discount FLAG")
fact_sales_raw = fact_sales_raw.drop("Customer Feedback")
fact_sales_raw = fact_sales_raw.drop("Warranty ID")
fact_sales_raw = fact_sales_raw.drop("Employee ID")
fact_sales_raw = fact_sales_raw.drop("Store ID")
fact_sales_raw = fact_sales_raw.drop("Progress Status ID")
fact_sales_raw = fact_sales_raw.drop("Delivery Channel ID")

fact_sales_raw = fact_sales_raw.withColumn("Total Amount", regexp_replace(col("Total Amount"),"\\$",""))
fact_sales_raw = fact_sales_raw.withColumn("Tax Amount", regexp_replace(col("Tax Amount"),"\\$",""))
fact_sales_raw = fact_sales_raw.withColumn("Shipping Cost", regexp_replace(col("Shipping Cost"),"\\$",""))
fact_sales_raw = fact_sales_raw.withColumn("Manufactory Costs", regexp_replace(col("Manufactory Costs"),"\\$",""))
fact_sales_raw = fact_sales_raw.withColumn("Profit", regexp_replace(col("Profit"),"\\$",""))

fact_sales_raw = fact_sales_raw.withColumn("Total Amount", trim(col("Total Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Tax Amount", trim(col("Total Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Shipping Cost", trim(col("Total Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Manufactory Costs", trim(col("Total Amount")))
fact_sales_raw = fact_sales_raw.withColumn("Profit", trim(col("Total Amount")))


configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "e47e4fc2-f5dd-4139-8138-6b6660cbe1c9",
  "fs.azure.account.oauth2.client.secret": "8ki8Q~Wao16rZXOpVpJRXt0NjuFQmRSLNFvh6bz9",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/05af7fc5-5a44-41d1-af78-3f2ca0e2514c/oauth2/token"
}

dbutils.fs.unmount("/mnt/fact_sales_raw")
dbutils.fs.mount(
      source="abfss://transformed-data@dataengineeringk.dfs.core.windows.net",
      mount_point="/mnt/fact_sales_raw",
      extra_configs=configs)

fact_sales_raw= fact_sales_raw.repartition(1)
      
fact_sales_raw.write.option("header", "true").csv("/mnt/fact_sales_raw/fact_sales_transformed")

# COMMAND ----------

fact_sales_raw.show()
