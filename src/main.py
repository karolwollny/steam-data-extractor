import os

from steam.webapi import WebAPI as SteamWebAPI
from pyspark.sql import SparkSession, functions as F

#########################################################
##### GLOBALS
#########################################################
PAGE_SIZE = 50000
STEAM_API_KEY = os.environ.get("STEAM_API_KEY")


#########################################################
##### FUNCTIONS
#########################################################
# TODO: Add function body to parse args
def parse_args():
    pass

def get_api_data(steam_api: SteamWebAPI,
                 method_path: str,
                 last_appid: int =-1,
                 max_results: int = PAGE_SIZE
                 ) -> dict:
    results = steam_api.call(
        method_path=method_path,
        last_appid=last_appid,
        max_results=max_results
    )
    return results['response']

def get_steam_apps_batch(steam_api: SteamWebAPI, method_path: str, last_appid: int = 0, max_results: int = PAGE_SIZE) -> dict:
    return get_api_data(steam_api, method_path, last_appid, max_results)['apps']

def get_all_steam_apps(steam_api: SteamWebAPI, method_path: str, last_appid: int = 0, max_results: int = PAGE_SIZE) -> dict:
    results = []

    new_results = get_steam_apps_batch(steam_api=steam_api, method_path='IStoreService.GetAppList')
    results.extend(new_results)
    last_id = new_results[-1]['appid']

    while len(new_results) >= PAGE_SIZE:
        new_results = get_steam_apps_batch(steam_api=steam_api,
                                           method_path='IStoreService.GetAppList',
                                           last_appid=last_id)
        last_id = new_results[-1]['appid']
        results.extend(new_results)

    print(f"Got {len(results)} games from steam. ")
    return results

def create_spark_session_if_needed():
    """
    Creates a Spark session if it doesn't already exist.
    If running on DataBricks, Spark Session is created by automation and passed to script/notebook.
    :return: SparkSession
    """
    try:
        return spark
    except NameError:
        spark = (SparkSession.builder
                 .config("spark.eventLog.enabled", "true")
                 .appName("steam-data-extractor")
                 .enableHiveSupport() # enable hive metastore to be used instead of default spark metastore
                 .config("spark.executor.memory", "1G")
                 .config("spark.executor.cores", "1")
                 .config("spark.sql.catalogImplementation", "hive")  # force Spark to use Hive catalog
                 .config("spark.eventLog.dir", "file:/tmp/spark-events")
                 .master("spark://localhost:7077")
                 .getOrCreate())

        spark.sparkContext.setLogLevel("ERROR") # change spark logging level to decrease number of logs
        return spark


#########################################################
##### MAIN FUNCTION
#########################################################
def main():
    steam_api = SteamWebAPI(key=STEAM_API_KEY)
    spark = create_spark_session_if_needed()

    results = get_all_steam_apps(steam_api=steam_api,
                                 method_path='IStoreService.GetAppList')

    df = spark.createDataFrame(results)
    df.persist() # persist the df due to count action
    print(f"Number of apps got from Steam api: {df.count()}")

    df = df.withColumn("last_modified_timestamp", df.last_modified.cast("timestamp"))
    df = df.withColumn("last_modified_year", F.year(df.last_modified_timestamp))

    # Create 'steam' schema in warehouse/metastore if it doesn't exist
    spark.sql("CREATE SCHEMA IF NOT EXISTS STEAM;")

    (df.coalesce(1) # coalesce to reduce number of output files to 1; not needed if there is bucketBy
     .write
     .mode("overwrite")
     #.bucketBy(numBuckets=6, col="last_modified_year")
     #.partitionBy(['last_modified_year']# current dataset size is ~5mb thus no partitioning needed
     .saveAsTable(name="steam.steam_games", format='parquet', mode='overwrite')) # TODO: Add option to save data as table

    df.unpersist()


#########################################################
##### RUNTIME
#########################################################
if __name__ == "__main__":
    main()