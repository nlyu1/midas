import psutil
from functools import reduce
import math
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
import dill


def optional_pbar(iterator, use_pbar, total_len=None):
    if use_pbar:
        return tqdm(iterator, total=total_len)
    return iterator


def chunk_list(lst, chunk_size, assert_div=True):
    """
    Splits a list into smaller lists (chunks) of a specified size.

    Parameters:
    -----------
    lst : list
        The list to be split into chunks.
    chunk_size : int
        The size of each chunk.
    assert_div : bool, optional, default=True
        If True, raises a ValueError if the length of the list is not divisible by chunk_size.
        If False, the final chunk may be smaller than chunk_size, containing all remaining elements.

    Returns:
    --------
    list of lists
        A list where each element is a chunk of the original list, with each chunk having
        size chunk_size, except possibly the last one if assert_div is False.

    Raises:
    -------
    ValueError
        If assert_div is True and the length of the list is not divisible by chunk_size.

    Examples:
    ---------
    >>> chunk_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

    >>> chunk_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 4, assert_div=False)
    [[1, 2, 3, 4], [5, 6, 7, 8], [9]]

    >>> chunk_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 4, assert_div=True)
    ValueError: List length 9 is not divisible by chunk size 4.
    """
    if assert_div and len(lst) % chunk_size != 0:
        raise ValueError(
            f"List length {len(lst)} is not divisible by chunk size {chunk_size}."
        )

    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def get_num_threads(logical=False):
    return psutil.cpu_count(logical)


class ParallelMap:
    def __init__(
        self, max_workers=None, pbar=False, use_thread=True, mp_context="spawn"
    ):
        """
        Initialize parallel mapping executor

        Parameters:
        -----------
        max_workers : int, optional
            Maximum number of parallel workers
        pbar : bool, optional
            Whether to show progress bar
        use_thread : bool, optional
            Whether to use threads (False uses processes)
        mp_context : str, optional
            Multiprocessing context ('fork', 'spawn', 'forkserver')
            'spawn' is more reliable but slower, 'fork' is faster but may have issues
        """
        self.max_workers = get_num_threads() if max_workers is None else max_workers
        self.pbar = pbar
        self.use_thread = use_thread
        self.mp_context = mp_context

    def __call__(self, fn, args, pbar=True):
        results = [None] * len(args)

        if self.use_thread:
            # Use regular ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(fn, arg): index for index, arg in enumerate(args)
                }
                for future in optional_pbar(
                    as_completed(futures), self.pbar and pbar, total_len=len(args)
                ):
                    index = futures[future]
                    results[index] = future.result()
        else:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(fn, arg): index for index, arg in enumerate(args)
                }
                for future in optional_pbar(
                    as_completed(futures), self.pbar and pbar, total_len=len(args)
                ):
                    index = futures[future]
                    results[index] = future.result()
        return results

    def chunk_apply(self, fn, args, batch_size=None, num_chunks=None, pbar=True):
        if batch_size is not None and num_chunks is not None:
            raise RuntimeError(
                "batch_size and num_chunks cannot be specified at the same time"
            )
        if batch_size is None and num_chunks is None:
            num_chunks = self.max_workers
        if num_chunks is not None:
            batch_size = math.ceil(len(args) / num_chunks)

        chunked_args = chunk_list(args, batch_size, assert_div=False)
        num_batches = len(chunked_args)
        chunked_fn = lambda args_lst: [fn(args) for args in args_lst]
        results = self(chunked_fn, chunked_args, pbar)
        return reduce(lambda x, y: x + y, results)
