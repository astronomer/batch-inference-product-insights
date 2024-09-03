import requests
import os


def extract_local_files(
    source_unit: str,
    ingest_start_data_param_name: str = None,
    labels: list = None,
    **context,
) -> dict:
    """
    Reads text from local files (.md, text).
    Args:
        source_unit (str): URI of a folder containing files.
        ingest_start_data_param_name (str): Not used.
        labels (list): Not used.
    Returns:
        dict: Extracted text in the format
            {"source_unit": source_unit, "texts": [text1, text2, ...]}
    """

    uri = source_unit

    path = uri.split("file://")[1]

    files = os.listdir(path)
    files = [os.path.join(path, file) for file in files]

    info = {"source_unit": uri, "texts": []}
    for file in files:
        with open(file, "r") as f:
            text = f.read()

        info["texts"].append(text)

    # get the current context and define the custom map index variable
    from airflow.operators.python import get_current_context

    context = get_current_context()
    context["my_custom_map_index"] = f"Extracted files from {uri}"

    return info


def extract_issues_gh_repo(
    source_unit: str,
    ingest_start_data_param_name: str = None,
    labels: list = None,
    **context,
) -> dict:
    """
    Extracts issues from a GitHub repository.
    Pulls the date of the last ingestion from the Airflow context.
    Needs a GitHub token to authenticate provided as an
    environment variable GH_TOKEN.
    Args:
        source_unit (str): GitHub repository name.
        ingest_start_data_param_name (str): Name of the parameter in the context to
            fetch the last ingestion date.
        labels (list): List of labels to filter issues. Default is None=fetch all.
    Returns:
        dict: Extracted text in the format
            {"source_unit": source_unit, "texts": [text1, text2, ...]}
    """

    ingest_start_date = context["params"][ingest_start_data_param_name]
    repo_name = source_unit
    token = os.getenv("GH_TOKEN")

    if labels:
        labels_param = ",".join(labels)
        url = f"https://api.github.com/repos/{repo_name}/issues?state=all&since={ingest_start_date}&labels={labels_param}"

    else:
        url = f"https://api.github.com/repos/{repo_name}/issues?state=all&since={ingest_start_date}"

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    r = requests.get(url, headers=headers)

    info = {"source_unit": repo_name, "texts": []}

    for issue in r.json():

        url = issue["url"]

        if "pulls" not in url:  # the endpoint can also fetch PRs we don't want those
            title = issue["title"]
            body = issue["body"]

            text = f"GitHub Issue Title: {title}, GitHub Issue Body: {body}"
            info["texts"].append(text)

    # get the current context and define the custom map index variable
    from airflow.operators.python import get_current_context

    context = get_current_context()
    context["my_custom_map_index"] = f"Extracted issues from {repo_name}"

    return info


def extract_so_questions(
    source_unit: str,
    ingest_start_data_param_name: str = None,
    labels: list = None,
    **context,
) -> dict:
    """
    Extracts questions from StackOverflow.
    Pulls the date of the last ingestion from the Airflow context.
    Args:
        source_unit (str): StackOverflow tag.
        ingest_start_data_param_name (str): Name of the parameter in the context to
            fetch the last ingestion date.
        labels (list): Not used.
    Returns:
        dict: Extracted text in the format
            {"source_unit": source_unit, "texts": [text1, text2, ...]}
    """

    ingest_start_date = context["params"][ingest_start_data_param_name]

    url = "https://api.stackexchange.com/2.3/questions"
    params = {
        "order": "desc",
        "sort": "creation",
        "tagged": source_unit,
        "site": "stackoverflow",
        "pagesize": 10,
        "page": 1,
        "filter": "withbody",
        "creation": ingest_start_date,
    }
    response = requests.get(url, params=params)

    info = {"source_unit": source_unit, "texts": []}

    for question in response.json()["items"]:
        title = question["title"]
        body = question["body"]
        text = title + "\n" + body
        info["texts"].append(text)

    # get the current context and define the custom map index variable
    from airflow.operators.python import get_current_context

    context = get_current_context()
    context["my_custom_map_index"] = f"Extracted questions for the tag {source_unit}"

    if response.status_code == 200:
        return info
    else:
        raise Exception(f"Failed to fetch data from SO API: {response.status_code}")
