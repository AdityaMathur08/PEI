from pyspark.sql.functions import col
from pyspark.sql.functions import to_date


def standardize_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").replace(",", "").replace(";", "").replace("{", "").replace("}", "").replace("(", "").replace(")", "").replace("\n", "").replace("\t", "").replace("=", "")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

def transform_date(df, column_name):
    df = df.withColumn(column_name, to_date(df[column_name], 'd/M/yyyy'))
    return df