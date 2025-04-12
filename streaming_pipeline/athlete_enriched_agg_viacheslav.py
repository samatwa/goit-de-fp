import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, current_timestamp, when, isnan, lit, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

# Function for logging
def log_message(message):
    print(f"[LOG] {message}")
    sys.stdout.flush()  # Make sure the message is displayed immediately

# Get environment variables or use defaults
mysql_host = os.environ.get("MYSQL_HOST", "217.61.57.46")
mysql_port = os.environ.get("MYSQL_PORT", "3306")
mysql_db = os.environ.get("MYSQL_DB", "neo_data")
mysql_user = os.environ.get("MYSQL_USER", "neo_data_admin")
mysql_password = os.environ.get("MYSQL_PASSWORD", "Proyahaxuqithab9oplp")
kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "77.81.230.104:9092")
kafka_username = os.environ.get("KAFKA_USERNAME", "admin")
kafka_password = os.environ.get("KAFKA_PASSWORD", "VawEzo1ikLtrA8Ug8THa")

# Initialize Spark Session with increased memory and timeouts
log_message("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("Olympic Athletes Data Processing") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.default.parallelism", "20") \
    .config("spark.jars", 
            "/home/samatwa77/goit-de-fp/jars/commons-pool2-2.11.1.jar,"
            "/home/samatwa77/goit-de-fp/jars/kafka-clients-3.5.0.jar,"
            "/home/samatwa77/goit-de-fp/jars/mysql-connector-java-8.0.29.jar,"
            "/home/samatwa77/goit-de-fp/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
            "/home/samatwa77/goit-de-fp/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
           ) \
    .getOrCreate()

log_message("Spark session created successfully")

# 1. Read athlete_bio data from MySQL with timeouts and size limits
log_message("Reading athlete_bio data from MySQL...")
try:
    athlete_bio_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}?connectTimeout=30000&socketTimeout=30000") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "olympic_dataset.athlete_bio") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("fetchsize", "10000") \
        .load()
    
    bio_count = athlete_bio_df.count()
    log_message(f"athlete_bio data loaded. Row count: {bio_count}")
except Exception as e:
    log_message(f"Error loading athlete_bio data: {str(e)}")
    sys.exit(1)

# 2. Filter out records with empty height and weight values
try:
    cleaned_bio_df = athlete_bio_df.filter(
        (col("height").isNotNull()) & 
        (~isnan(col("height"))) &
        (col("weight").isNotNull()) & 
        (~isnan(col("weight")))
    )
    
    cleaned_count = cleaned_bio_df.count()
    log_message(f"Cleaned bio data. Row count: {cleaned_count}")
except Exception as e:
    log_message(f"Error filtering bio data: {str(e)}")
    sys.exit(1)

# 3a. Read athlete_event_results data from MySQL and write to Kafka
log_message("Reading athlete_event_results from MySQL...")
try:
    athlete_events_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}?connectTimeout=30000&socketTimeout=30000") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "olympic_dataset.athlete_event_results") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("fetchsize", "10000") \
        .load()
    
    events_count = athlete_events_df.count()
    log_message(f"athlete_event_results data loaded. Row count: {events_count}")
except Exception as e:
    log_message(f"Error loading athlete_event_results data: {str(e)}")
    sys.exit(1)

# Convert DataFrame to JSON and write to Kafka with smaller batches
log_message(f"Connecting to Kafka: {kafka_bootstrap_servers} with user {kafka_username}")
log_message("Writing event data to Kafka...")
try:
    # Split data into smaller parts
    athlete_events_df = athlete_events_df.repartition(20)
    
    athlete_events_df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", "athlete_event_results") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.max.block.ms", "60000") \
        .option("kafka.batch.size", "16384") \
        .option("kafka.linger.ms", "5") \
        .save()
    
    log_message("Data written to Kafka topic: athlete_event_results")
except Exception as e:
    log_message(f"Error writing to Kafka: {str(e)}")
    sys.exit(1)

# 3b. Read data from Kafka topic with smaller batch size
log_message("Starting streaming from Kafka...")
try:
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", "athlete_event_results") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "1000") \
        .option("failOnDataLoss", "false") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
        .load()
except Exception as e:
    log_message(f"Error starting Kafka stream: {str(e)}")
    sys.exit(1)

# Define schema based on athlete_event_results table structure
event_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True)
])

# Parse JSON from Kafka
try:
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), event_schema).alias("data")) \
        .select("data.*")
except Exception as e:
    log_message(f"Error parsing Kafka data: {str(e)}")
    sys.exit(1)

# 4. Join event results with athlete biological data
try:
    joined_df = parsed_df.alias("event").join(
        cleaned_bio_df.alias("bio"),
        on="athlete_id",
        how="inner"
    ).select(
        "event.sport", "event.medal", "event.country_noc",
        "bio.sex", "bio.height", "bio.weight"
    )
except Exception as e:
    log_message(f"Error joining data: {str(e)}")
    sys.exit(1)

# 5. Calculate average values by sport, medal, sex, and country
try:
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
            round(avg("height"), 2).alias("avg_height"),
            round(avg("weight"), 2).alias("avg_weight")
        ) \
        .withColumn("timestamp", current_timestamp())
except Exception as e:
    log_message(f"Error aggregating data: {str(e)}")
    sys.exit(1)

# 6. Stream results with improved error handling
def process_batch(batch_df, batch_id):
    try:
        log_message(f"Processing batch {batch_id}")
        
        if batch_df.rdd.isEmpty():
            log_message("Batch is empty, skipping")
            return
        
        # Display sample data for diagnostics
        log_message("Sample data from batch:")
        try:
            batch_df.show(2, truncate=False)
        except Exception as e:
            log_message(f"Error showing sample data: {str(e)}")
        
        # a) Write to output Kafka topic
        try:
            batch_df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", "athlete_enriched_agg") \
                .option("kafka.security.protocol", "SASL_PLAINTEXT") \
                .option("kafka.sasl.mechanism", "PLAIN") \
                .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
                .option("kafka.request.timeout.ms", "60000") \
                .option("kafka.max.block.ms", "60000") \
                .save()
            log_message("Successfully wrote to Kafka topic: athlete_enriched_agg")
        except Exception as e:
            log_message(f"Error writing to Kafka: {str(e)}")
        
        # b) Write to MySQL with specified table name
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}?connectTimeout=30000&socketTimeout=30000") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("dbtable", "viacheslav.athlete_enriched_agg_viacheslav") \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("createTableColumnTypes",
                        "sport VARCHAR(100), medal VARCHAR(50), sex VARCHAR(10), country_noc VARCHAR(10), avg_height DOUBLE, avg_weight DOUBLE, timestamp TIMESTAMP") \
                .mode("append") \
                .save()
            
            log_message("Successfully wrote to MySQL table: viacheslav.athlete_enriched_agg_viacheslav")
        except Exception as e:
            log_message(f"Error writing to MySQL: {str(e)}")
    except Exception as e:
        log_message(f"Error in process_batch: {str(e)}")

log_message("Streaming query started. Awaiting termination...")
try:
    # Set shorter trigger interval and timeout for completion
    query = aggregated_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .start()
    
    # Set timeout for waiting for termination
    query.awaitTermination(timeout=600)
    
    log_message("Streaming query completed successfully")
except Exception as e:
    log_message(f"Error in streaming query: {str(e)}")
    sys.exit(1)