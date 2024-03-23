from preprocessing.job import ExecuteJob
from utils.functions.base_functions import EnvironmentFunctions

def execute(chat_id: str, bot_id: str, aws_access_key: str, aws_secret_key: str, s3_bucket: str, s3_file: str, datapath: str):
    # Create EnvironmentFunctions object
    env = EnvironmentFunctions()

    # Call _create_spark_session() to create a SparkSession
    spark = env._create_spark_session(aws_access_key, aws_secret_key)

    # Create boto3 Client to access AWS S3 bucket
    s3_client = env._create_boto3_client()

    # Create dataframes
    dataframe = env._create_source_dataframes(s3_bucket=s3_bucket, s3_file=s3_file, file=datapath, spark=spark)

    # Create ExecuteJob object
    exec_job = ExecuteJob()

    # Execute run() method from ExecuteJob Class passing a few parameters
    exec_job.run(spark, dataframe, s3_client, chat_id, bot_id)

if __name__ == "__main__":
    execute()