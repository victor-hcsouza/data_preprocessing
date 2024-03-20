import os
from preprocessing.job import ExecuteJob
from utils.functions.base_functions import EnvironmentFunctions

current_dir = os.path.dirname(os.path.abspath(__file__))
datapath = current_dir+"/data/spotify-2023.csv"

def execute():
    # Create EnvironmentFunctions object
    env = EnvironmentFunctions()

    # Call _create_spark_session() to create a SparkSession
    spark = env._create_spark_session()

    # Create dataframes
    dataframe = env._create_source_dataframes(file=datapath, spark=spark)

    # Create ExecuteJob object
    exec_job = ExecuteJob()

    # Execute run() method from ExecuteJob Class passing SparkSession as a parameter
    exec_job.run(spark, dataframe)

if __name__ == "__main__":
    execute()