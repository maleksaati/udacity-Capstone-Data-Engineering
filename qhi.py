from pyspark.sql import functions as F

def cast_totype(df, cols, type):
    """
    cast columns to specfied type
    
    Args:
        df : Spark dataframe 
        cols: list of columns need to be converted
        type : datatype to be converted to
    
    Returns:
        dataframe: The dataframe with specified column casted to type.
    """
    for c in cols:
        df = df.withColumn(c, df[c].cast(type))
    return df


def data_exists(df, table_name):
    """
    check count of table to check for data available
 
    Args:
        df: spark dataframe.
        table_name: name of table.
 
    Returns:
        int: The total count of rows in table.
    """
    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {table_name}, zero records found!")
    else:
        print(f"Data quality check passed for {table_name}, available records counts: {total_count} ")
    return total_count

def check_integrity(immigration_df, i49visa_df, transportation_mode_df, demographics_df, countries_df):
    """
    Chek the integrity of Fact table with dimesions tables
    
    Args:
    immigration_df: immigration Fact table.
    i49visa_df: visa type dim table.
    transportation_mode_df: trasportation mode dim table. 
    demographics_df: demographics dim table.
    countries_df: countries dim table.


    :return: true or false if integrity is correct.
    """
    integrity_demographics = immigration_df.select(F.col("i94addr")).distinct() \
                            .join(demographics_df, immigration_df["i94addr"] == demographics_df["State Code"], "left_anti") \
                            .count() == 0


    integrity_countries = immigration_df.select(F.col("i94res")).distinct() \
                                .join(countries_df, immigration_df["i94res"] == countries_df["Code"],
                                    "left_anti") \
                                .count() == 0

    integrity_visatype = immigration_df.select(F.col("i94visa")).distinct() \
                            .join(i49visa_df, immigration_df["i94visa"] == i49visa_df["vid"], "left_anti") \
                            .count() == 0

    integrity_transmode = immigration_df.select(F.col("i94mode")).distinct() \
                            .join(transportation_mode_df, immigration_df["i94mode"] == transportation_mode_df["i94mode"], "left_anti") \
                            .count() == 0

    if(integrity_demographics==0):
        print("integrity test failed for table demographics")
    else:
        print("integrity test passed for table demographics")

    if(integrity_countries==0):
        print("integrity test failed for table coutries")
    else:
        print("integrity test passed for table coutries")

    if(integrity_visatype==0):
        print("integrity test failed for table visatype")
    else:
        print("integrity test passed for table visatype")

    if(integrity_transmode==0):
        print("integrity test failed for table tasnportation mode")
    else: 
        print("integrity test passed for table transportation mode")
    
    return integrity_demographics & integrity_countries & integrity_visatype & integrity_transmode
