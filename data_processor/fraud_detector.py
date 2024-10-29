from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import from_json, col, hour, lit, udf
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import numpy as np
from datetime import datetime, timedelta
import psycopg2



spark = SparkSession.builder \
    .appName("KafkaFraudDetection") \
    .getOrCreate()


def get_db_connection():
    conn = psycopg2.connect(
        host = "timescaledb",
        database = "fraud_database",
        user = "user",
        password = "password"
    )
    return conn

def get_timescale_connection():
    return{
        "url": "jdbc:postgresql://timescaledb:5432/fraud_detection",
        "driver": "org.postgresql.Driver",
        "user": "user",  
        "password": "password",
        "dbtable": "account_details"
    }


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


