import shutil
from pathlib import Path


def printv(content: str, verbose=True):
    if verbose:
        print(content)


def verify_directory(
    path: Path, assert_empty=False, delete_contents=False, verbose=True
) -> None:
    """Verify and prepare a directory with specified constraints.

    Args:
        path: Directory path to verify/create
        assert_empty: Raise error if directory exists and is not empty
        delete_contents: Delete directory contents if it exists
        verbose: Print status messages

    Raises:
        RuntimeError: If path exists but is not a directory, or if assert_empty
                     is True and directory is not empty
        ValueError: If both assert_empty and delete_contents are True
    """
    # Validate contradictory parameters
    if assert_empty and delete_contents:
        raise ValueError("Cannot use both assert_empty=True and delete_contents=True")

    # Handle directory deletion if requested
    if delete_contents and path.exists():
        if not path.is_dir():
            raise RuntimeError(f"Expected {path} to be a directory, but found file")
        shutil.rmtree(path)
        printv(f"Deleted existing content at {path}", verbose)

    # Create directory if it doesn't exist (handles race condition with exist_ok)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        printv(f"Created new {path}", verbose)
        return

    # Verify path is a directory
    if not path.is_dir():
        raise RuntimeError(f"Expected {path} to be a directory, but found file")

    # Check if directory is empty (if required)
    if assert_empty:
        contents = list(path.iterdir())  # iterdir() catches hidden files too
        if contents:
            raise RuntimeError(f"Expected {path} to be empty. Contents: {contents}")
