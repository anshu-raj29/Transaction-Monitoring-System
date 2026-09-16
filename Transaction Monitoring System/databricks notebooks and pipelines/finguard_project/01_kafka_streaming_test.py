# Databricks notebook source
# topic='credit_card_transactions'

# COMMAND ----------

import json
kafka_connection_json=dbutils.secrets.get(scope="finguard-scope",key="kafka_connection_details")
kafka_config=json.loads(kafka_connection_json)
bootstrap_servers=kafka_config['bootstrap_servers']
api_key=kafka_config['api_key']
api_secret=kafka_config['api_secret']
topic=kafka_config['topic']

jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'

# read - used for batch read
sample_batch=(spark.read.format("kafka")
              .option("kafka.bootstrap.servers",bootstrap_servers)
              .option("subscribe",topic)

              # connection requirements
              .option("kafka.security.protocol", "SASL_SSL")     # Authentication Protocols
              .option("kafka.sasl.mechanism", "PLAIN")           # mechanishm
              .option("kafka.sasl.jaas.config", jaas_config)     # java authentication & authorization service
              .option("startingOffsets","earliest")              # to shows data from start of the topic to latest(earliest)
              .load()
)

# count of records frond start to latest/now
sample_batch.count()

# display the dataframe
# data is shown as key-value pair in databricks (transaction-amount)
# Key & Value gets converted to binary. To see actual data, convert them to string.
# partition = 6 (0-5)
# offset = position of data within that partition
display(sample_batch)

# convert binary key-value pair to string
from pyspark.sql.functions import col
parsed_batch=sample_batch.select(
col("key").cast("string"),
col("value").cast("string"),
col("topic"),
col("partition"),
col("offset"),
col("timestamp"),
col("timestampType")
)

# display the converted dataframe
display(parsed_batch)

# to save dataframe as table (bronze)
parsed_batch.write.saveAsTable("finguard.bronze.transactions_batch_test")

# for reading we specify reading dataframe -> readstream
# readStream - changes from a batch read to a stream read
# it can't be displayed -> so, display(stream_df) will throw an error 
streaming_df=(spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers",bootstrap_servers)
            .option("subscribe",topic)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas_config)
            .option("startingOffsets","earliest")
            .load()
            )

# it can't be displayed -> so, display(stream_df) will throw an error 
# to handle this error, we write the streaming data into a streaming table and check the data there

# convert into streaming dataframe
from pyspark.sql.functions import col
parsed_streaming_df=streaming_df.select(
col("key").cast("string"),
col("value").cast("string"),
col("topic"),
col("partition"),
col("offset"),
col("timestamp"),
col("timestampType")
)

# convert streaming dataframe into streaming table
# checkpoint location = location where it will store the progress of the stream. 
# once it read the data it write it in table and when for next data it has to remmebr from which offset it has to read the data. 
# so to store that progress it has to store that checkpoint location
# for locaton checkpoint, we have to create a volume (directory)
# inside this volume we create a directory/folder -> checkpoint

# for writing we specify streaming dataframe -> writestream
streaming_query=(parsed_streaming_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/finguard/source/transactions/checkpoint/")
.trigger(availableNow=True) # load all the new data that is available now. if we run it for the first time it will load everything that is avilable.if we reun it for second time, it will check at checkpoint location what data it has read reviously & that point onwards it will read the new data that is available now n stop
.toTable("finguard.bronze.transactions_streaming_test") # specify the table name that we want to create.
)
print("Query Started with query id: ",streaming_query.id) # id used for debuging and tracking purpose

# MAGIC %sql
# MAGIC select * from finguard.bronze.transactions_streaming_test

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.silver.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.gold.transaciton_count_by_minute

# MAGIC %sql
# MAGIC select * from finguard.gold.transaciton_count_by_minute_sliding_window