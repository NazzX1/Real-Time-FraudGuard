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



# spark = SparkSession.builder \
#     .appName("KafkaFraudDetection") \
#     .getOrCreate()



_ = load_dotenv(find_dotenv())



def get_db_connection():
    conn_str = os.environ["TIMESCALE_SERVICE_URL"]
    conn = psycopg2.connect(conn_str)
    return conn


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
    
    model_features = ['distance_from_home', 'distance_from_last_transaction',
                      'ratio_to_median_purchase_price', 'repeat_retailer',
                      'used_chip', 'used_pin_number', 'online_order']
    

    transaction_df = pd.DataFrame([transaction])

    is_fraud = model.predict(transaction_df)[0]

    return bool(is_fraud) 



transaction_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "transactions") \
    .load()



transaction_data = transaction_stream.selectExpr("CAST(value AS STRING)")
transaction_json = transaction_data.select(from_json(col("value"), schema).alias("transaction_data"))




transaction_df = transaction_json.select("transaction_data.*")



try:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM conditions")
            results = cursor.fetchall()
            print(results)
except Exception as e:
    print(f"An error occurred: {e}")

