import configparser
from pyspark import SparkConf
import pandas as pd


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    # fixme - check for config path + file validity
    config.read("spark.conf")
    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def get_column_names():
    # todo - Manipulate this array for right combination for Composite keys
    columns_array = ['name', 'address', 'city', 'cuisine']
    return columns_array


def get_pandas_df(file_name, sheet_name):
    # fixme check for filepath validity / put try except block
    df = pd.read_excel(file_name, sheet_name=sheet_name)
    df = df.astype(str)
    return df


def merge_result_with_query(query, result):
    return pd.merge(query, result, how='outer', left_on="id", right_on="id")


def save_results_to_excel(filename, reference_df, result_df):
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        reference_df.to_excel(writer, "reference", index=False)
        result_df.to_excel(writer, "query", index=False)
        writer.save()
    return True
