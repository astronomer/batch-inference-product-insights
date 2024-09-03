"""
### Aggregate Feedback per Product

This DAG is designed to aggregate feedback per product or feature, 
summarize the feedback, and send the summaries to a Slack channel.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.io.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from pendulum import datetime
import json
import logging
import os

t_log = logging.getLogger("airflow.task")

# S3 bucket and connection
_AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_default")
_S3_BUCKET = os.getenv("S3_BUCKET", "my-bucket")
_OBJECT_STORAGE_SRC = os.getenv("OBJECT_STORAGE_SRC", "s3")

_SRC_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/tagged_feedback/tags.json",
    conn_id=_AWS_CONN_ID,
)

_DST_PATH = ObjectStoragePath(
    f"{_OBJECT_STORAGE_SRC}://{_S3_BUCKET}/product_summaries/",
    conn_id=_AWS_CONN_ID,
)

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🤖 Aggregate and summarize feedback",
    start_date=datetime(2024, 9, 1),
    schedule=[Dataset(_SRC_PATH.as_uri())],
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
def aggregate_feedback_per_product():

    # ---------------- #
    # Task definitions #
    # ---------------- #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", outlets=[Dataset("x-data-summarized")])

    @task(multiple_outputs=False)
    def get_file_list_per_product() -> dict:
        """
        Reads the tags.json file from remote storage and returns the content
        as an inverted dictionary.
        Returns:
            dict: Dictionary with the content of the tags.json file.
        """

        # read from remote storage
        bytes = _SRC_PATH.read_block(offset=0, length=None)
        text_file = bytes.decode("utf-8")

        config = json.loads(text_file)

        inverted_data = {}
        for key, value in config.items():
            if value not in inverted_data:
                inverted_data[value] = [key]
            else:
                inverted_data[value].append(key)

        return inverted_data

    @task
    def get_list_of_products(inverted_data: dict) -> list:
        """
        Extracts the list of products from the inverted dictionary.
        Args:
            inverted_data (dict): Inverted dictionary with products as keys.
        Returns:
            list: List of products.
        """

        products = list(inverted_data.keys())

        return products

    @task(
        multiple_outputs=False,
        map_index_template="{{ my_custom_map_index }}",
    )
    def aggregate_feedback_per_product(product: str, inverted_data: dict) -> dict:
        """
        Aggregates feedback per product.
        Args:
            product (str): Product name.
            inverted_data (dict): Inverted dictionary with products as keys.
        Returns:
            dict: Dictionary with the product and its feedback.
        """

        files = inverted_data[product]

        feedback = {}
        for i, file in enumerate(files):
            bytes = ObjectStoragePath(file).read_block(offset=0, length=None)
            feedback[f"Feedback user {i}"] = bytes.decode("utf-8")

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Fetching feedback for {product}."

        return {"product": product, "feedback": feedback}

    @task(map_index_template="{{ my_custom_map_index }}", multiple_outputs=False)
    def summarize_feedback(input: dict) -> dict:
        """
        Summarizes feedback for a product or feature.
        Args:
            input (dict): Dictionary with the product and its feedback.
        Returns:
            dict: Dictionary with the product, summaries, and timestamp.
        """
        from openai import OpenAI
        from include.functions.utils.token_wrangling import (
            count_tokens,
            split_text_to_fit_tokens,
            format_feedback,
        )

        t_log.info(f"Summarizing feedback for product/feature: {input['product']}")
        t_log.info(f"Raw feedback input: {input['feedback']}")

        feedback = format_feedback(input["feedback"])
        product = input["product"]

        system_prompt = """
        You are an AI assistant helping a product team to summarize feedback, score sentiment 
        and gather product insights for the managed Airflow service 'Astro by Astronomer' 
        as well as specific Airflow features.

        You will get a list of feedback for {product} each individual 
        feedback is separated by `--------`. Focus on feedback directly relating to 
        the product {product} in your analysis.

        Score sentiment on a scale from 0 to 1, where 0 is max negative and 1 is max positive.
        Preserve technical language and specific feature requests.

        Return the result as a JSON object with the following structure:
        {{
            "feedback_summary": "Everyone loves Datasets and wishes for more functionality, especially their integration with Airflow sensors.",
            "sentiment_score": 0.8
            "insights": ["Datasets are well liked", "Add sensor functionality to Datasets."]
        }}
        """

        user_prompt = feedback

        ## Split the user prompt into chunks if token limit is exceeded

        max_tokens = 5000  # Define the max tokens per API call, adjust as needed

        total_tokens = count_tokens(user_prompt)

        if total_tokens > max_tokens:
            user_prompts = split_text_to_fit_tokens(
                user_prompt,
                max_tokens - count_tokens(system_prompt.format(product=product)),
            )
        else:
            user_prompts = [user_prompt]

        t_log.info(f"System prompt: {system_prompt}")

        summaries = []
        client = OpenAI()

        for user_prompt in user_prompts:
            t_log.info(f"User prompt: {user_prompt}")

            chat_completion = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": system_prompt.format(product=product),
                    },
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
            )
            summaries.append(json.loads(chat_completion.choices[0].message.content))

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = (
            f"Feedback summary for product/feature: {product}"
        )

        # get dag run timestamp
        timestamp = context["ts"]

        summary_dict = {
            "product": product,
            "summaries": summaries,
            "timestamp": timestamp,
            "product": product,
        }

        # write summary dict to s3
        summary_dict_str = json.dumps(summary_dict, ensure_ascii=False)
        summary_dict_bytes = summary_dict_str.encode()

        file_path = _DST_PATH / f"{product}.json"
        file_path.write_bytes(summary_dict_bytes)

        return summary_dict

    @task(map_index_template="{{ my_custom_map_index }}")
    def send_summaries_to_slack(input: list) -> None:
        """
        Sends the summaries to a Slack channel.
        Args:
            input (dict): Dictionary with the product, summaries, and timestamp.
        """
        import os
        from airflow.providers.slack.hooks.slack import SlackHook
        import textwrap
        import json

        product = input["product"]
        summaries = input["summaries"]

        formatted_summaries = []

        for summary in summaries:

            if isinstance(summary, str):
                summary = json.loads(summary)

            summary_text = "\n".join(
                textwrap.wrap(summary.get("feedback_summary", ""), width=80)
            )
            sentiment_score = summary.get("sentiment_score", 0)
            insights = summary.get("insights", "")

            formatted_summary = {
                "feedback summary": summary_text,
                "sentiment score": sentiment_score,
                "insights": insights,
            }

            formatted_summaries.append(formatted_summary)

            if isinstance(insights, list):
                insights_formatted = "\n".join(f"• {item}" for item in insights)
            else:
                insights_formatted = insights

            SLACK_MESSAGE = (
                f"------------------------------------------------\n\n"
                f"Hey there, a wild new product summary has appeared!. 😊\n\n"
                f"----------------\n\n"
                f"📝 The summary:\n\n"
                f"{summary_text}\n\n"
                f"----------------\n\n"
                f"🙂 Sentiment score: {formatted_summary['sentiment score']}\n\n"
                f"----------------\n\n"
                f"💡 Insights:\n"
                f"{insights_formatted}\n"
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

        # Write the formatted summaries to a JSON file
        with open(f"include/product_summaries/{product}.json", "w") as f:
            json.dump(formatted_summaries, f, indent=4)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Sent feedback for {product}"

    # ------------------- #
    # Call task functions #
    # ------------------- #

    list_per_product = get_file_list_per_product()
    products = get_list_of_products(inverted_data=list_per_product)
    agg_feedback = aggregate_feedback_per_product.partial(
        inverted_data=list_per_product
    ).expand(product=products)
    summaries = summarize_feedback.expand(input=agg_feedback)
    write_summaries = send_summaries_to_slack.expand(input=summaries)

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        start, list_per_product, products, agg_feedback, summaries, write_summaries, end
    )


aggregate_feedback_per_product()
