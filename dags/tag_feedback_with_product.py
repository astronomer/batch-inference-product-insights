"""
### Tag Feedback with Product

This DAG is a simple example of how to use the OpenAI API to classify 
product feedback based on features or products mentioned in the feedback. 
The DAG reads text files from two different sources, `api_data` and 
`zendesk_tickets`, and then uses the OpenAI API to classify the feedback. 
The classification is based on a list of possible classes, 
which are defined in the `CLASSES_LIST` variable. 
The result is saved as a JSON containing the file path and the product 
or feature name.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from pendulum import datetime
import os
import logging

t_log = logging.getLogger("airflow.task")


CLASSES_LIST = os.getenv("CLASSES_LIST")

# S3 bucket and connection
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
_OBJECT_STORAGE_SRC = os.getenv("OBJECT_STORAGE_SRC", "s3")

_SRC_PATH_API_DATA = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/ingest/api_data/", conn_id=_AWS_CONN_ID
)

_SRC_PATH_ZD_TICKETS = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/ingest/zendesk_tickets/",
    conn_id=_AWS_CONN_ID,
)

_DST_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/tagged_feedback/tags.json",
    conn_id=_AWS_CONN_ID,
)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🤖 Tag Feedback with Product/Feature",
    start_date=datetime(2024, 9, 1),
    schedule=(
        Dataset(_SRC_PATH_API_DATA.as_uri()) | Dataset(_SRC_PATH_ZD_TICKETS.as_uri())
    ),
    default_args={
        "owner": "ML Engineering",
        "retries": 2,
        "retry_delay": 60 * 5,
    },
    catchup=False,
    doc_md=__doc__,
    description="ML",
    tags=["OpenAI", "classification"],
)
def tag_feedback_with_product():

    # ---------------- #
    # Task definitions #
    # ---------------- #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset(_DST_PATH.as_uri())])

    @task
    def get_texts_api_data() -> list:
        """
        Get the file paths for the text files in the `api_data` key.
        Returns:
            list: List of file paths.
        """
        path = _SRC_PATH_API_DATA
        files = [f for f in path.iterdir() if f.is_file()]
        return files

    @task
    def get_texts_zendesk() -> list:
        """
        Get the file paths for the text files in the `zendesk_tickets` key.
        Returns:
            list: List of file paths.
        """
        path = _SRC_PATH_ZD_TICKETS
        files = [f for f in path.iterdir() if f.is_file()]
        return files

    file_paths = get_texts_api_data().concat(get_texts_zendesk())

    @task(map_index_template="{{ my_custom_map_index }}")
    def product_identifier(file: ObjectStoragePath, option_list=None) -> dict:
        """
        Classify product feedback based on features or products mentioned in the feedback.
        Args:
            file (ObjectStoragePath): The file path to read the feedback from.
            option_list (list): List of possible classes.
        Returns:
            dict: Classification results in the format
                {
                    "file": file_path,
                    "product": product_name_or_feature_name
                }.
        """
        from openai import OpenAI
        import json

        # read from remote storage
        bytes = file.read_block(offset=0, length=None)
        text_file = bytes.decode("utf-8")

        system_prompt = f"""
        You are an AI assistant helping users to classify product feedback based on features or products mentioned in the feedback.
        If the classification is uncertain or ambiguous, use the label 'NOT SPECIFIED'.
        These are all possible classes: {option_list}.

        Return the result as a JSON object with the key being the file_path and the value being the product or feature name.

        Example Response 1:
        {{
            "product": "Datasets"
        }}

        Example Response 2:
        {{
            "product": "Dynamic Task Mapping"
        }}

        Example Response 3:
        {{
            "product": "NOT SPECIFIED"
        }}
        """

        user_prompt = text_file

        client = OpenAI()

        t_log.info(f"System prompt: {system_prompt}")
        t_log.info(f"User prompt: {user_prompt}")

        chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )

        response = chat_completion.choices[0].message.content

        t_log.info(f"Response: {response}")

        response_dict = json.loads(response)
        response_dict["file"] = file.as_uri()

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Classifying feedback from {file}."

        return response_dict

    @task
    def save_tags(tags: list) -> None:
        """
        Write tags to remote storage.
        Args:
            tags (list): List of tag dictionaries.
        """
        import json

        # Filter out tags that are not in the classes list
        tags = {
            tag["file"]: tag["product"]
            for tag in tags
            if tag["product"] in CLASSES_LIST
        }

        print(tags)

        tags_str = json.dumps(tags, ensure_ascii=False)
        tags_bytes = tags_str.encode()
        _DST_PATH.write_bytes(tags_bytes)

    product_identification = product_identifier.partial(
        option_list=CLASSES_LIST
    ).expand(file=file_paths)

    tags = save_tags(product_identification)

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        start,
        file_paths,
        product_identification,
        tags,
        end,
    )


tag_feedback_with_product()
