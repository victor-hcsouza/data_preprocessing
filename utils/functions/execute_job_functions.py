import pyspark.sql.functions as F

from utils.functions.base_functions import BaseFunctions
from typing import Dict


class Job(BaseFunctions):
    def __init__(self, task_name: str, query: str, chat_id: str, bot_id: str) -> None:
        super().__init__()
        self.task_name = task_name
        self.query = query
        self.chat_id = chat_id
        self.bot_id = bot_id

    def execute_query(
        self, query: str, sparkSession, query_args: Dict[str, str] = None
    ):
        result = self.run_sql_query(query, query_args, sparkSession)

        return result