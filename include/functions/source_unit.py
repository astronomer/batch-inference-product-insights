import os


def get_local_files_source_units(uri: str) -> list:
    """
    Returns a list of folders in the given URI.
    Args:
        uri (str): URI of a folder containing files.
    Returns:
        list: List of folders in the given URI.
    """
    path = uri.split("file://")[1]
    folders = os.listdir(path)
    folders = [os.path.join(uri, folder) for folder in folders]
    return folders


def get_gh_repos(list_of_repos: list) -> list:
    """
    Returns the list of GH repos passed unchanged.
    Args:
        list_of_repos (list): List of GH repos.
    Returns:
        list: The list of GH repos passed unchanged.
    """
    return list_of_repos


def get_so_tags(list_of_tags: list) -> list:
    """
    Returns the list of SO tags passed unchanged.
    Args:
        list_of_tags (list): List of SO tags.
    Returns:
        list: The list of SO tags passed unchanged."""
    return list_of_tags
