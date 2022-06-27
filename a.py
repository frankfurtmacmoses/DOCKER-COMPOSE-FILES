from pyspark.sql import SparkSession

def my_job():
        spark = (SparkSession
        .builder
        .appName("demo-lakehouse")
        .master("spark://192.168.1.111:7077")
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') 
        .config('spark.jars.packages', 'org.apache.kafka:kafka-clients:2.8.1') 
        .config('spark.jars.packages','org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2') 
        .getOrCreate())

        df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "192.168.1.111:9092") \
        .option("subscribe", "COURSE") \
        .option("startingOffsets", "earliest") \
        .load()
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        df.show()
  
if __name__ == '__main__':
    my_job()
    
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 a.py
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 org.apache.kafka:kafka-clients:2.8.1 org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py 
    
    #spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py 