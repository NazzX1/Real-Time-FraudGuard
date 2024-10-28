from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import from_json, col, hour, lit, udf
import pickle
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import numpy as np
from datetime import datetime, timedelta



spark = SparkSession.builder \
    .appName("KafkaFraudDetection") \
    .getOrCreate()


