from airflow.io.path import ObjectStoragePath


def load_text_chunks_to_object_storage(info: dict, dst_path: ObjectStoragePath, **context) -> None:
    """
    Load text chunks to object storage.
    Args:
        info (dict): Dictionary containing information.
        dst_path (ObjectStoragePath): Path to the destination folder.
    """

    source_unit_identifier = info["source_unit"].replace("/", "_")

    for i, text in enumerate(info["texts"]):

        new_file = dst_path / f"{source_unit_identifier}_{i}.txt"
        text_bytes = text[0].encode()
        new_file.write_bytes(text_bytes)

    # get the current context and define the custom map index variable
    from airflow.operators.python import get_current_context

    context = get_current_context()
    context["my_custom_map_index"] = f"Load info from {source_unit_identifier} to {dst_path}."
