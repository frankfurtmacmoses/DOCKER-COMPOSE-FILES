from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pyspark.sql.avro.functions import from_avro


def my_job():
        spark = (SparkSession
        .builder
        .appName("demo-lakehouse")
        .master("spark://192.168.1.111:7077") 
        .getOrCreate())

        df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.111:9092") \
        .option("subscribe", "school.dbo.Course") \
        .option("startingOffsets", "earliest") \
        .load()
        df.printSchema()
        jsonFormatSchema = open("./schema.avsc","r").read()
        finaldata = df.select(from_avro(data=df.value,jsonFormatSchema=jsonFormatSchema).alias("avro"))
        finaldata.printSchema()
        finaldata.writeStream.format("csv").option("path","./").option("checkpointLocation", "./work").outputMode("append").start()
        
      
      
        

if __name__ == '__main__':
    my_job()
    
    
    
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 a.py --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py
    
    
    # spark-submit --jars jar_files/spark-avro_2.12-3.1.3.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 a.py --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py
    
  