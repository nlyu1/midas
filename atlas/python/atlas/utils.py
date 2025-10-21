import shutil
from pathlib import Path


def printv(content: str, verbose=True):
    if verbose:
        print(content)

def printv_lazy(content_lambda, verbose=True):
    if verbose:
        print(content_lambda())
