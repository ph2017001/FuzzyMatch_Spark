import configparser
from pyspark import SparkConf
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer


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


def create_pipeline():
    """Pipeline with following stages
    1. lowercase,
    2. tokenize
    3. stopwords removal
    4. concat
    5. regex tokenize
    6. ngram creation
    7. hashingTF
    8. MinHashLSH
    The pipeline should transform the "Composite_Key" column of the input dataframe"""
    pipeline = Pipeline(stages=[
        SQLTransformer(statement="SELECT *, lower(Composite_Key) lower FROM __THIS__"),
        Tokenizer(inputCol="lower", outputCol="token"),
        StopWordsRemover(inputCol="token", outputCol="stop"),
        SQLTransformer(statement="SELECT *, concat_ws(' ', stop) concat FROM __THIS__"),
        RegexTokenizer(pattern="", inputCol="concat", outputCol="char", minTokenLength=1),
        NGram(n=2, inputCol="char", outputCol="ngram"),
        HashingTF(inputCol="ngram", outputCol="vector"),
        MinHashLSH(inputCol="vector", outputCol="lsh", numHashTables=3)
    ])
    return pipeline


def merge_result_with_query(query, result):
    return pd.merge(query, result, how='outer', left_on="id", right_on="id")


def save_results_to_excel(filename, reference_df, result_df):
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        reference_df.to_excel(writer, "reference", index=False)
        result_df.to_excel(writer, "query", index=False)
        writer.save()
    return True
