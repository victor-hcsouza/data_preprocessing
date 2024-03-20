import requests
import pyspark.sql.functions as F
import great_expectations as Ge
import os

from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from typing import Dict

#This is very important to ignore Airflow's proxy and send the message
os.environ["no_proxy"]="*"

class BaseFunctions:
    def send_telegram_notifications(self, chat_id: str, bot_id: str, message: str) -> None:
        url = f"https://api.telegram.org/bot{bot_id}/sendMessage"
        dados = {
            "chat_id": chat_id,
            "text": message
        }

        response = requests.get(url, json=dados, timeout=10)

        if response.status_code == 200:
            print("Message sent successfully")
        else:
            print("Message not sent:", response.text)

    def cast_df_columns(self, dataframe: DataFrame) -> DataFrame:
        casted_df = dataframe
        for column in casted_df.columns:
            if column.startswith("t"):
                casted_df = casted_df.withColumnRenamed(column, "nm_"+column)
        
        return casted_df

    def check_df_quality(self, dataframe: DataFrame):
        dataframe = dataframe.withColumn("primaryKey", F.concat_ws("|", F.col("nm_track_name"), F.col("artist_name")))

        ge_df = Ge.dataset.SparkDFDataset(dataframe)

        response = ge_df.expect_column_values_to_be_unique(column='primaryKey')
        return response

    def run_sql_query(
        self, query: str, query_args: Dict[str, str], spark: SparkSession
    ):
        df = spark.sql(query, query_args)

        return df
    
    def create_temp_views(self,
        temp_view_args: Dict[DataFrame, str]
    ):
        for df, name_view in temp_view_args.items():
            df.createOrReplaceTempView(name_view)


class EnvironmentFunctions:
    @staticmethod
    def _create_spark_session() -> SparkSession:
        try:
            spark = SparkSession.builder \
                .master("local[*]") \
                .appName("MeuApp") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()

            print("PySpark environment created sucessfully\n")

            return spark
        except Exception as e:
            print(e)

    def _create_source_dataframes(self, file, spark: SparkSession = None) -> DataFrame:
        try:
            format = file.split('.')[-1]
            
            if format == 'csv':
                df_csv = spark.read.csv(file, header=True, inferSchema=True).repartition(10)

                print("Dataframe created successfully\n")
                return df_csv

            elif format == 'json':
                df_json = spark.read.json(file)

                print("Dataframe created successfully\n")
                return df_json
        
            elif format == 'parquet':
                df_parquet = spark.read.parquet(file)

                print("Dataframe created successfully\n")
                return df_parquet
            
            else:
                raise ValueError(f"Not supported format '{format}'.")
            
        except Exception as e:
            raise Exception(e)