import shutil
from pathlib import Path


def printv(content: str, verbose=True):
    if verbose:
        print(content)
