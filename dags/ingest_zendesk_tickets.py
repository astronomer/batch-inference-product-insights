"""
### Ingest Zendesk Tickets DAG

This DAG ingests Zendesk tickets from a Snowflake database and writes them to 
object storage.
It will safe the latest ticket ID in an Airflow Variable to keep track of the
last ingestion and only ingest new tickets.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
import logging
import os

t_log = logging.getLogger("airflow.task")

# S3 bucket and connection
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
_OBJECT_STORAGE_SRC = os.getenv("OBJECT_STORAGE_SRC", "s3")

_DST_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/ingest/zendesk_tickets/",
    conn_id=_AWS_CONN_ID,
)

_INGEST_TICKETS_TASK_ID = "get_latest_tickets"
_SKIP_INGEST_TASK_ID = "skip_ingest"

# Snowflake variables
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB", "product_insights_demo")
_SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "dev")
_SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "zd_tickets")

_TICKET_ID_VAR_NAME = "MOST_RECENT_ZD_TICKET"

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="Ingest ZD tickets from ❄️",
    start_date=datetime(2024, 9, 1),
    schedule=Dataset("x-start-ZD"),  # supposed to be manually triggered!
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    default_args={
        "owner": "Data Engineering",
        "retries": 2,
        "retry_delay": 60 * 5,
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ingest"],
)
def ingest_zendesk_tickets():

    # ---------------- #
    # Task definitions #
    # ---------------- #

    get_latest_ticket_id_obj = SQLExecuteQueryOperator(
        task_id="get_latest_ticket_id",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=f"""
            SELECT TICKET_ID 
            FROM {_SNOWFLAKE_DB}.{_SNOWFLAKE_SCHEMA}.{_SNOWFLAKE_TABLE} 
            WHERE TICKET_ID >= {{{{ var.value.get('{_TICKET_ID_VAR_NAME}', '0') }}}} 
            ORDER BY TICKET_ID DESC
            LIMIT 1;
        """,
        show_return_value_in_logs=True,
    )

    @task.branch
    def is_new_tickets(
        latest_ticket_id: int, ingest_tickets_task_id: str, skip_ingest_task_id: str
    ) -> str:
        """
        Check if there are new tickets to ingest.
        Args:
            latest_ticket_id (int): Latest ticket ID in Snowflake.
            ingest_tickets_task_id (str): Task ID to ingest new tickets.
            skip_ingest_task_id (str): Task ID to skip ingestion.
        Returns:
            str: Task ID to execute.
        """

        last_run_latest_ticket = int(Variable.get(_TICKET_ID_VAR_NAME, "0"))
        latest_ticket_id = latest_ticket_id[0][0]
        if latest_ticket_id == last_run_latest_ticket:
            t_log.info("No new tickets!")
            return skip_ingest_task_id
        elif latest_ticket_id > last_run_latest_ticket:
            t_log.info("New tickets to ingest!")
            return ingest_tickets_task_id
        else:
            raise Exception(
                f"""
                Latest ticket ID: {latest_ticket_id} is lower than the last run's latest ticket id {last_run_latest_ticket}.
                """
            )

    is_new_tickets_obj = is_new_tickets(
        latest_ticket_id=get_latest_ticket_id_obj.output,
        ingest_tickets_task_id=_INGEST_TICKETS_TASK_ID,
        skip_ingest_task_id=_SKIP_INGEST_TASK_ID,
    )

    get_latest_tickets_obj = SQLExecuteQueryOperator(
        task_id=_INGEST_TICKETS_TASK_ID,
        conn_id=_SNOWFLAKE_CONN_ID,
        sql=f"""
            SELECT ticket_id, subject, description 
            FROM {_SNOWFLAKE_DB}.{_SNOWFLAKE_SCHEMA}.{_SNOWFLAKE_TABLE} 
            WHERE TICKET_ID > {{{{ var.value.get(MOST_RECENT_ZD_TICKET, '0') }}}} 
            ORDER BY TICKET_ID DESC;
        """,
        show_return_value_in_logs=True,
    )

    @task(outlets=[Dataset(_DST_PATH.as_uri())])
    def write_tickets_to_object_storage(latest_ticket_info: list) -> None:
        """
        Write the latest ticket info to object storage.
        Args:
            latest_ticket_info (list): List of latest tickets.
        """

        for ticket in latest_ticket_info:
            ticket_id = ticket[0]
            title = ticket[1]
            text = ticket[2]

            new_file = _DST_PATH / f"{ticket_id}.txt"
            new_file.write_bytes(title.encode() + b"\n" + text.encode())

    write_tickets_to_object_storage_obj = write_tickets_to_object_storage(
        latest_ticket_info=get_latest_tickets_obj.output
    )

    @task
    def update_latest_ticket_id(latest_ticket_id: list) -> None:
        """
        Update the latest ticket id in the Airflow Variable.
        Args:
            latest_ticket_id (list): List containing the latest ticket id.
        """
        prev = Variable.get(_TICKET_ID_VAR_NAME, "0")
        t_log.info(f"Previous latest ticket id: {prev}")
        new = latest_ticket_id[0][0]
        Variable.set(_TICKET_ID_VAR_NAME, new)
        t_log.info(f"New latest ticket id: {new}")

    skip_ingestion = EmptyOperator(task_id=_SKIP_INGEST_TASK_ID)

    update_latest_ticket_id_obj = update_latest_ticket_id(
        get_latest_ticket_id_obj.output
    )

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        get_latest_tickets_obj,
        [update_latest_ticket_id_obj, write_tickets_to_object_storage_obj],
    )

    chain(
        get_latest_ticket_id_obj,
        is_new_tickets_obj,
        [get_latest_tickets_obj, skip_ingestion],
    )


ingest_zendesk_tickets()
