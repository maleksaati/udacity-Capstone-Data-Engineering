

def quality_checks(df, table_name):
    """check count of table to check for data available

    :param df: spark dataframe
    :param table_name: name of table
    """
    total_count = df.count()

    if total_count == 0:
        print(f"Data quality check failed for {table_name}, zero records found!")
    else:
        print(f"Data quality check passed for {table_name}, available records counts: {total_count} ")
    return 0