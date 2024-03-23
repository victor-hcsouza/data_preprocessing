import traceback

from utils.functions.execute_job_functions import Job
from utils.sql_queries.query import query

class ExecuteJob:
    def run(self, spark, dataframe, s3_client, chat_id, bot_id):
        try:
            print("Initiating process...\n")

            job = Job(
                task_name="Spotify report with pyspark and python",
                query=query,
                chat_id=f"{chat_id}",
                bot_id=f"{bot_id}"
            )

            print("Creating temp views...\n")
            job.create_temp_views(
                {dataframe: "db"}
            )

            print("Executing query...\n")
            df = job.execute_query(
                query=query,
                sparkSession=spark,
                query_args={"track_name": "Cruel Summer"},
            )

            print("Renaming columns...\n")
            df = df.withColumnRenamed("artist(s)_name", "artist_name")

            print("Casting columns...\n")
            casted_df = job.cast_df_columns(
                dataframe=df
            )

            print("Checking dataframe quality...\n")
            response = job.check_df_quality(
                dataframe=casted_df
            )

            if response["success"] != True:
                print("Error caught...\n")
                raise ValueError(response)
            
            print("Job executed. Sending message...\n")
            msg = f"JOB: {job.task_name} EXECUTED \U00002705"

            casted_df.show()

            job.send_telegram_notifications(
                chat_id = job.chat_id,
                bot_id=job.bot_id,
                message=msg
            )

        except Exception as e:
            tracer = traceback.format_exc()
            detailed_msg = f"```{e}\n{tracer}```"
            headline = f"{25*'-'}\n\U0001F47A ERROR: {job.task_name}*\n{25*'-'}\n\n"
            formatted_msg = headline + detailed_msg
            print(formatted_msg)
            job.send_telegram_notifications(
                chat_id = job.chat_id,
                bot_id=job.bot_id,
                message=formatted_msg
            )