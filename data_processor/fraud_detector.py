from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import from_json, col, hour, lit, udf
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import numpy as np
from datetime import datetime, timedelta
import psycopg2
import os
from dotenv import load_dotenv, find_dotenv



spark = SparkSession.builder \
    .appName("FraudDetection") \
    .getOrCreate()



_ = load_dotenv(find_dotenv())



def get_db_connection():
    try:
        conn_str = os.environ["TIMESCALE_SERVICE_URL"]
        conn = psycopg2.connect(conn_str)
        return conn
    except Exception as e:
        print(f'Failed to connect to database : {e}')
        return None


schema = StructType([
    StructField("distance_from_home", FloatType(), True),
    StructField("distance_from_last_transaction", FloatType(), True),
    StructField("ratio_to_median_purchase_price", FloatType(), True),
    StructField("repeat_retailer", BooleanType(), True),
    StructField("used_chip", BooleanType(), True),
    StructField("used_pin_number", BooleanType(), True),
    StructField("online_order", BooleanType(), True),
])




# Load the pre-trained model
with open('fraud_detection.pkl', 'rb') as model:
    model = pickle.load(model)


def detect_fraud(dfh, dflt, rmpp, repeat_retailer, used_chip, used_pin_number, online_order):

    transaction = {
                    'distance_from_home': dfh,
                    'distance_from_last_transaction' : dflt,
                    'ratio_to_median_purchase_price': rmpp,
                    'repeat_retailer': repeat_retailer,
                    'used_chip': used_chip,
                    'used_pin_number': used_pin_number,
                    'online_order': online_order, 
                }

    transaction_df = pd.DataFrame([transaction])

    is_fraud = model.predict(transaction_df)[0]

    return bool(is_fraud)


fraud_udf = udf(detect_fraud, BooleanType())



transaction_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "transactions") \
    .load()



transaction_data = transaction_stream.selectExpr("CAST(value AS STRING)")
transaction_json = transaction_data.select(from_json(col("value"), schema).alias("transaction_data"))




transaction_df = transaction_json.select("transaction_data.*")



def save_to_database(batch_df):

    batch_df_local = batch_df.collect()  # Collect locally to avoid Spark-Psycopg2 compatibility issues
    try:
        conn = get_db_connection()
        if conn is None:
            raise Exception("Failed to establish TimescaleDB connection")
        with conn.cursor() as cursor:
            for row in batch_df_local:
                cursor.execute("""
                    INSERT INTO transactions (distance_from_home, distance_from_last_transaction,
                    ratio_to_median_purchase_price, repeat_retailer, used_chip, used_pin_number,
                    online_order, is_fraud)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.distance_from_home, row.distance_from_last_transaction,
                    row.ratio_to_median_purchase_price, row.repeat_retailer,
                    row.used_chip, row.used_pin_number, row.online_order, row.is_fraud
                ))
            conn.commit()
        print("Batch inserted successfully into TimescaleDB")
    except Exception as e:
        print(f"Error saving to TimescaleDB: {e}")
    finally:
        if conn:
            conn.close()

query = transaction_df.writeStream \
    .foreachBatch(save_to_database) \
    .outputMode("append") \
    .start()

query.awaitTermination()