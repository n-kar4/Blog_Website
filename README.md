import sys
import time
import re
from pyspark.context import SparkContext
from datetime import datetime, timedelta
import pandas as pd


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Processing") \
    .getOrCreate()

# Load the CSV files into Spark DataFrames
source_df = spark.read.csv('./easy_calender.csv',header=True)
# target_df = spark.read.csv('./tgt.csv', header=True, inferSchema=True)


# Register the DataFrames as temporary views
source_df.createOrReplaceTempView("srcdata")
# target_df.createOrReplaceTempView("tgtdata")

# display the data
print("source data")
# source_df.show()

#filter rows with type_date = "finh"
spark.sql("SELECT * FROM srcdata WHERE trim(TYPE_DATE) in ('FINH', 'WEND')").show()
finh_df = spark.sql("SELECT * FROM srcdata WHERE trim(TYPE_DATE) in ('FINH', 'WEND')")
finh_df.createOrReplaceTempView("finhdata")


# print("finh data")
# finh_df.show()

# VCH-DT,,U,,,45667,,X(10),,Y,yyyy-mm-dd,53,62,10,,,,"If the date in the file name falls on a Monday, Voucher Date (VCH-DT) = date in file name - 3 days (verify that if Friday 
# is a Holiday, subtract one more day.)""Else Voucher Date (VCH-DT) = date in file name - 1 day (verify that if date is a Holiday, subtract one 
# more day.)",,,,,,

#check if it is holiday and return true of false
def is_holiday(date):
    # Convert the string to a datetime object
    date_obj = datetime.strptime(date, "%Y-%m-%d")

    # Format the datetime object to the desired format
    date = date_obj.strftime("%d-%b-%y")
    print(date)
    spark.sql(f"SELECT * FROM finhdata WHERE CALENDAR_DATE = '{date}'")
    ret = spark.sql(f"SELECT * FROM finhdata WHERE CALENDAR_DATE = '{date}'")
    print("ret",ret.count())
    if ret.count() > 0:
        return True
    else:
        return False


#calculate the vch date from sec date by checking the finh_df
def calculate_vch_date(sec_date):
    # Convert the date to datetime
    sec_date = datetime.strptime(sec_date, '%Y-%m-%d')
    print(sec_date, "in calculate_vch_date")
    
    vch_date = sec_date - timedelta(days=1)
    # Check if the date is a holiday
    while is_holiday(vch_date.strftime('%Y-%m-%d')):
        # Subtract one more day
        vch_date = vch_date - timedelta(days=1)

    
    return vch_date.strftime("%Y-%m-%d")

src_date = "2025-01-25"
print("vsh date for",src_date," is ", calculate_vch_date(src_date))
print(is_holiday(src_date))
