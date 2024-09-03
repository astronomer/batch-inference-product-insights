from langchain.text_splitter import RecursiveCharacterTextSplitter
from include.functions.utils.ignore_text import REPLACEMENT_LIST


def _replace_strings(text: str, replacements: list) -> str:
    """
    Replace strings in a single text based on replacement pairs.
    Args:
        text (str): The string to perform replacements on.
        replacements (list of tuple): List of (old, new) replacement pairs.
    Returns:
        str: The string after replacements.
    """
    for old, new in replacements:
        text = text.replace(old, new)
    return text


def transform_text(info: list[str], **context) -> list[str]:
    """
    Transform texts by applying specific replacements to each text.
    Args:
        texts (list of str): List of strings to transform.
    Returns:
        list of str: List of transformed strings.
    """
    info["texts"] = [_replace_strings(text, REPLACEMENT_LIST) for text in info["texts"]]

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=5000,
        chunk_overlap=100,
        length_function=len,
        separators=["\n\n", "\n", " ", ""],
    )

    info["texts"] = [text_splitter.split_text(text) for text in info["texts"]]

    # get the current context and define the custom map index variable
    from airflow.operators.python import get_current_context

    context = get_current_context()
    context["my_custom_map_index"] = f"Transformed texts from {info['source_unit']}."

    return info
