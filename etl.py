import pandas as pd
from pyspark.sql.functions import col,isnan, when, count,to_date
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark import SparkContext

import os 
import sys

from pyspark.sql.functions import udf
import datetime as dt

import configparser




def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")\
    .enableHiveSupport().getOrCreate()
    return spark


def cast_totype(df, cols, type):
    """
    cast columns to specfied type
    
    Args:
        df : Spark dataframe 
        cols: list of columns need to be converted
        type : datatype to be converted to
    """
    for c in cols:
        df = df.withColumn(c, df[c].cast(type))
    return df


def create_trans_mode_dim(spark, output_path):#Create Dim for Mode of transportation
# Create i94mode list

    schema = StructType([StructField("i94mode",IntegerType(),True),StructField("trans_mode",StringType(),True)])

    i94mode_data =([(1,"Air"),(2,"Sea"),(3,"Land"),(9,"Not reported")])

# Convert to spark dataframe
    i94mode=spark.createDataFrame(i94mode_data, schema=schema)
    #i94mode.show()

    i94mode.write.mode('overwrite').parquet(output_path + "/i94mode.parquet")
    return i94mode


def create_i94visa_dim(spark, output_path):
    I94VISA_schema = StructType([StructField("vid",IntegerType(),True),StructField("visatype",StringType(),True)])

    I94VISA_data =([(1,"Business"),(2,"Pleasure"),(3,"Student")])

    # Convert to spark dataframe
    I94VISA_df =spark.createDataFrame(I94VISA_data, schema=I94VISA_schema)
    #I94VISA_df.show()
    I94VISA_df.write.mode('overwrite').parquet(output_path + "/i94visa.parquet")
    I94VISA_df.show()
    return I94VISA_df


def create_demographics_dim(spark,output_path):
    us_demographics_df = spark.read.csv('data/us-cities-demographics.csv', sep=';', inferSchema=True, header=True)

    us_demographics_df.printSchema()

    int_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_cols = ['Median Age', 'Average Household Size']

    us_demographics_df = cast_totype(us_demographics_df, int_cols, IntegerType())
    us_demographics_df = cast_totype(us_demographics_df, float_cols, DoubleType())

    us_demographics_df.printSchema()
    
    #aggr_by_race_df = us_demographics_df.groupBy(["City", "State", "State Code"]).pivot("Race").agg( F.sum('Count'))
    us_demographics_df = us_demographics_df.groupBy(["City", "State", "State Code"]) \
    .agg(F.first("Median Age").alias('median_age'),F.first("Male Population").alias('male_population'), \
         F.first("Female Population").alias('female_population'),F.first("Total Population").alias('total_population'))
    
    #us_demographics_df = aggr_df2.join(other=aggr_by_race_df, on=["City", "State", "State Code"], how="inner")
    us_demographics_df.show()
    us_demographics_df.write.mode('overwrite').parquet(output_path + "/us_cities_demographics.parquet")
    return us_demographics_df



def create_immigration_dim(spark,output_path):
    immigration_df =spark.read.parquet("data/sas_data")
    immigration_df.show()

    immigration_df.dropDuplicates(['admnum']).count()

    # Performing cleaning tasks here

    #drop columns with more then 60% nulls (visapost, occup,entdepu,insnum, fltno)
    drop_list = ['visapost', 'occup','entdepu','insnum', 'fltno']
    immigration_df = immigration_df.drop(*drop_list)

    #Drop not needs columns
    no_needs_col = ["count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    immigration_df = immigration_df.drop(*no_needs_col)

    immigration_df.printSchema()
    # create a udf to convert arrival date in SAS format to datetime object
    #get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    #immigration_df = immigration_df.withColumn("arrival_date", get_datetime(immigration_df.arrdate))
    #immigration_df = immigration_df.withColumn("depature_date", get_datetime(immigration_df.depdate))

    immigration_df.show()

    immigration_df.write.mode("overwrite").parquet(output_path + "\immigration.parquet")
    return immigration_df
    


def create_temperature_dim(spark,output_path):
    fname = '../../data2/GlobalLandTemperaturesByCity.csv'
    temperature_df = spark.read.option("header", True).csv(fname)

    temperature_df = temperature_df.groupBy(["Country"]) \
    .agg(F.avg("AverageTemperature").alias('AverageTemperature'),F.first("Latitude").alias('Latitude'), \
         F.first("Longitude").alias('Longitude'))
    
    temperature_df = temperature_df.withColumn('Country', F.upper(temperature_df.Country))
    

    # Loads the lookup table I94CIT_I94RES
    ctry_df = spark.read.format('csv').options(header='true', inferSchema='true').load("data\I94CIT_I94RES.csv")    
    ctry_df.count()
    ctry_df = ctry_df.withColumn('I94CTRY', F.lower(ctry_df.I94CTRY))

    ctry_df = ctry_df.join(temperature_df, ctry_df.I94CTRY == temperature_df.Country, how="left")
    ctry_df.show()

    ctry_df =ctry_df.drop("Country")

    ctry_df.write.mode("overwrite").parquet(output_path + "\country.parquet")

    ctry_df.show()
    ctry_df.printSchema()

    return ctry_df
    


def create_date_dim(immigration_df,output_path):
    i94date_df =immigration_df.select(col('arrdate').alias('arrival_sasdate')).dropDuplicates()
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    i94date_df = i94date_df.withColumn("arrival_date", get_datetime(i94date_df.arrival_sasdate))
    i94date_df = i94date_df.withColumn('arrival_month',F.month(i94date_df.arrival_date))
    i94date_df = i94date_df.withColumn('arrival_year',F.year(i94date_df.arrival_date))
    i94date_df = i94date_df.withColumn('arrival_day',F.dayofmonth(i94date_df.arrival_date))
    i94date_df = i94date_df.withColumn('day_of_week',F.dayofweek(i94date_df.arrival_date))
    i94date_df = i94date_df.withColumn('arrival_weekofyear',F.weekofyear(i94date_df.arrival_date))

    i94date_df.write.mode("overwrite").parquet(output_path + "\i94date.parquet")

    return i94date_df


def run_pipeline():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # The AWS key id and password are configured in a configuration file "dl.cfg"

    config = configparser.ConfigParser()
    config.read('dl.cfg')



    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    #output_path = "s3a://udatalake/"
    output_path = "s3a//udatalake/"

    spark = create_spark_session()


    #create_trans_mode_dim(spark, output_path)
    i49visa_df = create_i94visa_dim(spark, output_path)
    demographics_df = create_demographics_dim(spark, output_path)
    immigration_df = create_immigration_dim(spark, output_path)
    countries_df = create_temperature_dim(spark, output_path)
    calendar_df = create_date_dim(immigration_df, output_path)

    return immigration_df, i49visa_df, demographics_df, countries_df, calendar_df


if __name__ == "__main__":
    run_pipeline()
    