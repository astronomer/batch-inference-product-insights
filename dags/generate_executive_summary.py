"""
### Generate Executive Summary DAG

This DAG generates an executive summary of product feedback summaries and insights.
And sends the summary to a Slack channel.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.io.path import ObjectStoragePath
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import os

# S3 bucket and connection
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
_OBJECT_STORAGE_SRC = os.getenv("OBJECT_STORAGE_SRC", "s3")

_SRC_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/product_summaries/",
    conn_id=_AWS_CONN_ID,
)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🤖 Executive summary",
    start_date=datetime(2024, 9, 1),
    schedule=[Dataset("x-data-summarized")],
    catchup=False,
    default_args={
        "owner": "ML Engineering",
        "retries": 2,
        "retry_delay": 60 * 5,
    },
    doc_md=__doc__,
    description="ML",
    tags=["summarization"],
)
def generate_executive_summary():

    # ---------------- #
    # Task definitions #
    # ---------------- #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("exec_summary")])

    @task
    def get_product_summary_filepaths() -> list:
        """
        Get the file paths for the product summary files. Filters NOT SPECIFIED files.
        Returns:
            list: List of file paths.
        """

        path = _SRC_PATH
        files = [
            f for f in path.iterdir() if f.is_file() and "NOT SPECIFIED" not in f.name
        ]

        return files

    @task
    def create_prompt_from_json_files(file_paths: list) -> str:
        """
        Create a prompt from the JSON files.
        Args:
            file_paths (list): List of ObjectStoragePaths.
        Returns:
            str: Prompt string.
        """
        import json

        prompt = ""
        separator = "\n-----------\n\n"

        for file_path in file_paths:

            file_content_bytes = file_path.read_block(offset=0, length=None)
            file_content_str = file_content_bytes.decode("utf-8")
            data = json.loads(file_content_str)

            file_path_name = file_path.name
            product = file_path_name.split(".")[0]

            for feedback_summary in data["summaries"]:

                summary = feedback_summary.get("feedback_summary", "")
                sentiment_score = feedback_summary.get("sentimentScore", "")
                insights = feedback_summary.get("insights", [])

                formatted_insights = "\n".join(insights)

                product_str = (
                    f"Product: {product}\n"
                    f"Feedback Summary: {summary}\n"
                    f"Sentiment Score: {sentiment_score}\n"
                    f"Insights:\n{formatted_insights}"
                )

                prompt += product_str + separator

        prompt = prompt.rstrip(separator)

        return prompt

    @task
    def generate_product_insight(product_summary: str) -> str:
        """
        Generate an executive summary from the product summary.
        Args:
            product_summary (str): Product summary string.
        Returns:
            str: Executive summary string.
        """

        from openai import OpenAI

        system_prompt = """
        You are an AI assistant that summarizes product feedback summaries and 
        insights at an executive level.  

        You will get a set of feedback summaries with sentiment scores and insights
        for different features/products. Each individual feedback summary is 
        is separated by `--------`.

        Create an overall executive summary of the feedback summaries and insights.
        Generate no more than 10 sentences, use buyer-friendly language.

        Return the result as a JSON object in the following format:
        {
            "executive_summary": "Your executive summary here."
        }
        """

        user_prompt = product_summary

        client = OpenAI()
        chat_completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
        )

        return chat_completion.choices[0].message.content

    @task
    def send_to_slack_executive_summary(insight: str) -> None:
        """
        Send the executive summary to a Slack channel.
        Args:
            insight (str): Executive summary.
        """
        from airflow.providers.slack.hooks.slack import SlackHook
        import textwrap
        import json

        executive_summary = json.loads(insight)["executive_summary"]

        formatted_summary = "\n".join(textwrap.wrap(executive_summary, width=80))

        SLACK_MESSAGE = (
            f"------------------------------------------------\n\n"
            f"Hey there, I have the latest executive summary for you. 😊\n"
            f"Here it is:\n\n"
            f"{formatted_summary}\n\n"
            f"------------------------------------------------\n\n"
        )
        SLACK_CONNECTION_ID = "slack_conn"
        SLACK_CHANNEL = "alerts"

        hook = SlackHook(slack_conn_id=SLACK_CONNECTION_ID)

        hook.call(
            api_method="chat.postMessage",
            json={
                "channel": SLACK_CHANNEL,
                "text": SLACK_MESSAGE,
            },
        )

    filepaths = get_product_summary_filepaths()
    product_summaries = create_prompt_from_json_files(filepaths)
    product_insights = generate_product_insight(product_summary=product_summaries)
    send_to_slack_executive_summary(insight=product_insights)


generate_executive_summary()
