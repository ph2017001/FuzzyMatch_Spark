import sys
from pyspark.sql import *
from pyspark.sql import functions as F, Window
from lib.logger import Log4j
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer

from lib.utils import get_spark_app_config, get_column_names, get_pandas_df, save_results_to_excel, \
    merge_result_with_query

if __name__ == "__main__":

    # Load the spark app configs from the spark.conf file,
    # Instantiate the conf object
    conf = get_spark_app_config()

    # Create the spark session object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Instantiate the Logger object
    logger = Log4j(spark)
    logger.info("App Configuration Loaded and Session Object Created")
    logger.info("Starting Fuzzy Match - Spark")

    # Check for Input File argument
    if len(sys.argv) != 2:
        logger.error("Usage: Fuzzy Match <filename>")
        sys.exit(-1)

    # Get reference, query sets/ pandas dataframes from the xlsx sheet
    reference_df_pd = get_pandas_df(file_name=sys.argv[1], sheet_name="reference")
    query_df_pd = get_pandas_df(file_name=sys.argv[1], sheet_name="query")
    logger.info("Input file parsed in pandas dataframes")

    # Get column names to combine and form the Composite_Key
    columns_array = get_column_names()

    # Create a Composite_Key column in reference and query sets
    reference_df_pd['Composite_Key'] = reference_df_pd[columns_array].apply(lambda x: ' '.join(x), axis=1)
    query_df_pd['Composite_Key'] = query_df_pd[columns_array].apply(lambda x: ' '.join(x), axis=1)

    # Create reference, query spark-dataframes from pandas-dataframes
    reference_df = spark.createDataFrame(reference_df_pd)
    query_df = spark.createDataFrame(query_df_pd)
    logger.info("Spark Dataframes created")

    # Configure an ML pipeline, which consists of eight stages:
    # 1. lowercase,
    # 2. tokenize
    # 3. stopwords removal
    # 4. concat
    # 5. regex tokenize
    # 6. ngram creation
    # 7. hashingTF
    # 8. MinHashLSH
    # The pipeline should transform the "Composite_Key" column of the input dataframe
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

    # Fit the pipeline to the training reference_df to create the model
    model = pipeline.fit(reference_df)
    logger.info("Model created by fitting to reference spark dataframe")

    # Using the model-pipeline previously defined, transform the reference and query dataframes (composite_key)
    reference_df_res = model.transform(reference_df)
    query_df_res = model.transform(query_df)

    # Filter out rows with zero size ngram
    query_df_res = query_df_res.filter(F.size(F.col("ngram")) > 0)
    reference_df_res = reference_df_res.filter(F.size(F.col("ngram")) > 0)

    # Setting the Jaccard Distance threshold
    # todo provide a knob to experiment with this threshold setting, pass as argument
    distance_threshold = 0.6

    # Using the last stage of the previously defined model-pipeline, perform a approxSimilarityJoin
    # between the transformed dataframes with the threshold set above and .
    # The join produces three columns : datasetA, datasetB, distanceColumn (as provided)
    # This join may return more number of rows corresponding to a single row in the left dataframe based on distance
    result = model.stages[-1].approxSimilarityJoin(query_df_res, reference_df_res, distance_threshold, "jaccardDist")

    # Logging input, result row counts
    logger.debug(f"{query_df_res.count()} rows in the left dataframe")
    logger.debug(f"{reference_df_res.count()} rows in the right dataframe ")
    logger.debug(f"{result.count()} rows in the approxSimilarityJoin result")

    # To select the join rows with the least distance score, running a Window Aggregation over the left dataframe id
    w = Window.partitionBy('datasetA.id')
    # Lower value of  Jaccard Distance implies better match
    result = (result
              .withColumn('minDist', F.min('jaccardDist').over(w))
              .where(F.col('jaccardDist') == F.col('minDist'))
              .drop('minDist'))

    # Logging the Matched Query/ Total Query for the set threshold value
    logger.info(f"with the distance threshold of {distance_threshold}, total {result.count()}/{query_df.count()}"
                f" query rows matched")

    # Showing first 5 results
    result.selectExpr(
        "datasetA.id as `id`",
        "datasetA.name as `name`",
        "datasetB.name as `ref_name`",
        "datasetB.id as `reference_id`",
        "round(jaccardDist*100,2) as `score(optional)`"
    ).show(5)

    # Select relevant columns from the result
    result = result.selectExpr(
        "datasetA.id as `id`",
        "datasetB.id as `reference_id`",
        "round(jaccardDist*100,2) as `score(optional)`"
    )

    # Convert the spark dataframe back to pandas dataframe
    result_pd = result.toPandas()

    # Drop Composite Key and redundant columns from the dataframes
    query_df_pd = query_df_pd.drop(columns=['Composite_Key', 'reference_id', 'score(optional)'], axis=1)
    reference_df_pd = reference_df_pd.drop(columns=['Composite_Key'], axis=1)

    # Join/Merge the result with the query set/dataframe
    final_result = merge_result_with_query(query_df_pd, result_pd)

    # Converting the strings back to integers
    reference_df_pd['id'] = reference_df_pd['id'].astype(int)
    final_result['id'] = final_result['id'].astype(int)
    final_result['reference_id'] = final_result['reference_id'].astype(int)

    # Persisting the results to the query sheet in the input file
    logger.info("Persisting the results to the query sheet in the input file.")
    save_results_to_excel(sys.argv[1], reference_df_pd, final_result)

    logger.info("Stopping FuzzyMatch - Spark")
    spark.stop()