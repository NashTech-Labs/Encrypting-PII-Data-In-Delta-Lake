# Databricks notebook source

# DBTITLE 1,Creating encryption key
from cryptography.fernet import Fernet 

encryptionKey = Fernet.generate_key()

# COMMAND ----------

# DBTITLE 1,Create Spark UDFs in python for encrypting a value
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text

# COMMAND ----------

# DBTITLE 1,Use the UDF in a dataframe to encrypt a productid column
from pyspark.sql.functions import udf, lit, md5
from pyspark.sql.types import StringType

encrypt = udf(encrypt_val, StringType())

encryptedDF = silverDF.withColumn("userId", encrypt("userId",lit(encryptionKey))) \
                      .withColumn("firstName", encrypt("firstName",lit(encryptionKey))) \
                      .withColumn("lastName", encrypt("lastName",lit(encryptionKey))) \
                      .withColumn("address", encrypt("address",lit(encryptionKey))) \
                      .withColumn("emailId", encrypt("emailId",lit(encryptionKey))) \
                      .withColumn("mobileNumber", encrypt("mobileNumber",lit(encryptionKey)))

# COMMAND ----------

# DBTITLE 1,Writing transformed silver data frame to the silver table
encryptedDF.writeStream.format("delta") \
.outputMode("append") \
.option("checkpointLocation", "/dbfs/pubsub-shippment-sliver-checkpoint/")
.trigger(processingTime = '5 seconds') \
.table("main.car_demo_data_lake.shipping_sliver")