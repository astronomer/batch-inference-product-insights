"""
### Ingest data from APIs and local files

Ingestion of product feedback from:
- Local files containing G2 reviews
- GitHub issues
- Stack Overflow questions

The feedback is cleaned and stored in text files in an S3 bucket.
This DAG is highly modularized with functions stored in the `include` folder and
ingestion paths being created from the `sources` list.

DAG params determine data from which sources are ingested. By default all
sources are toggled off.
"""

from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime, now
import os

# modularized functions
from include.functions import source_unit, extract, load, transform

# S3 bucket and connection
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
_OBJECT_STORAGE_SRC = os.getenv("OBJECT_STORAGE_SRC", "s3")

_DST_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/ingest/api_data/", conn_id=_AWS_CONN_ID
)

_SRC_PARAM_PREFIX = "source_"  # cannot start with `_`

if _SRC_PARAM_PREFIX[0] == "_":
    raise ValueError("Source param prefix _SRC_PARAM_PREFIX cannot start with _")

_INGEST_START_DATE_PARAM_NAME = "_ingest_start_date"
_INGEST_ALL_SOURCES_PARAM_NAME = "_ingest_all_sources"

# list of sources to ingest
sources = [
    {
        "name": "G2_reviews",
        "source_parameters": {
            "source_unit_function": source_unit.get_local_files_source_units,
            "kwargs": {"uri": "file://include/data"},
        },
        "extract_parameters": {
            "extraction_function": extract.extract_local_files,
            "kwargs": {},
        },
    },
    {
        "name": "gh_issues",
        "source_parameters": {
            "source_unit_function": source_unit.get_gh_repos,
            "kwargs": {"list_of_repos": ["apache/airflow"]},
        },
        "extract_parameters": {
            "extraction_function": extract.extract_issues_gh_repo,
            "kwargs": {"labels": ["kind:feature"]},  # filter by label
        },
    },
    {
        "name": "stackoverflow",
        "source_parameters": {
            "source_unit_function": source_unit.get_so_tags,
            "kwargs": {"list_of_tags": ["airflow"]},
        },
        "extract_parameters": {
            "extraction_function": extract.extract_so_questions,
            "kwargs": {},
        },
    },
]

# generate param toggles for each source
# by default all sources are toggled off
source_params = {
    f'{_SRC_PARAM_PREFIX}{src["name"]}': Param(
        False, type="boolean", description="Toggle on for ingestion!"
    )
    for src in sources
}

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="Ingest Data from APIs",
    start_date=datetime(2024, 9, 1),
    schedule=Dataset("x-start-APIs"),  # supposed to be manually triggered!
    catchup=False,
    max_consecutive_failed_dag_runs=5,
    params={
        _INGEST_START_DATE_PARAM_NAME: Param(
            "2024-06-01T00:00:00+00:00", type="string", format="date-time"
        ),
        _INGEST_ALL_SOURCES_PARAM_NAME: Param(
            False, type="boolean", description="Toggle on to ingest for all sources!"
        ),
        **source_params,  # toggles for each source generated from the sources list
    },
    default_args={
        "owner": "Data Engineering",
        "retries": 2,
        "retry_delay": 60 * 5,
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ingest"],
)
def ingest_data_apis():

    # ---------------- #
    # Task definitions #
    # ---------------- #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed",
        outlets=[Dataset(_DST_PATH.as_uri())],  # dataset to schedule downstream DAGs on
    )

    @task.branch
    def pick_ingestion_sources(**context):
        """
        Pick sources to ingest based on the params for manual runs.
        Returns a list of task_ids to run.
        """

        # get list of all params starting with the src related prefix
        list_of_all_src_params = [
            src for src in context["params"] if src.startswith(_SRC_PARAM_PREFIX)
        ]

        if context["params"][_INGEST_ALL_SOURCES_PARAM_NAME]:
            task_ids_to_run = [
                f"fetch_{src_name.replace(_SRC_PARAM_PREFIX, '')}_source_units"
                for src_name in list_of_all_src_params
            ]
            return task_ids_to_run
        else:
            list_of_true_src_params = [
                src for src in list_of_all_src_params if context["params"][src]
            ]

            task_ids_to_run = [
                f"fetch_{src_name.replace(_SRC_PARAM_PREFIX, '')}_source_units"
                for src_name in list_of_true_src_params
            ]

            return task_ids_to_run

    pick_ingestion_sources_obj = pick_ingestion_sources()

    # loop through the sources to create one set of sequential tasks for each source
    # a static loop is used because the task group contained is mapped dynamically
    # and you cannot nest dynamic mapping as of now (Airflow 2.10)
    for source in sources:

        # The first task fetches the source units _within_ a source for parallization
        # of the ingestion task group by using dynamic task group mapping
        source_units = task(
            source["source_parameters"]["source_unit_function"],
            task_id=f"fetch_{source['name']}_source_units",
            doc_md=source["source_parameters"]["source_unit_function"].__doc__,
        )(**source["source_parameters"]["kwargs"])

        @task_group(
            group_id=f"ingest_{source['name']}",
        )
        def ingestion(source_unit):
            """
            Task group containing the ingestion tasks for a source unit.
            Args:
                source_unit (str): The source unit from which to ingest data.
            """

            # the first task extracts feedback information from the source unit using
            # a modularized function in `include/functions/extract.py`
            texts = task(
                source["extract_parameters"]["extraction_function"],
                task_id=f"extract_{source['name']}",
                doc_md=source["extract_parameters"]["extraction_function"].__doc__,
                map_index_template="{{ my_custom_map_index }}",
            )(
                source_unit=source_unit,
                ingest_start_data_param_name=_INGEST_START_DATE_PARAM_NAME,
            )

            # the second task transforms the extracted texts
            transformed_texts = task(
                transform.transform_text,
                task_id=f"transform_{source['name']}",
                doc_md=transform.transform_text.__doc__,
                map_index_template="{{ my_custom_map_index }}",
            )(texts)

            # the third task loads the transformed texts to object storage
            task(
                load.load_text_chunks_to_object_storage,
                task_id=f"load_{source['name']}_chunks",
                doc_md=load.load_text_chunks_to_object_storage.__doc__,
                map_index_template="{{ my_custom_map_index }}",
            )(info=transformed_texts, dst_path=_DST_PATH)

        # calling the task group and dynamically mapping it over the source units
        # to create one task group per source unit within each source at runtime
        ingestion_tg = ingestion.expand(source_unit=source_units)

        # ------------------------------ #
        # Define additional dependencies #
        # ------------------------------ #

        chain(start, pick_ingestion_sources_obj, source_units, ingestion_tg, end)


ingest_data_apis()
