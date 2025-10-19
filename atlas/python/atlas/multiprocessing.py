from tqdm.auto import tqdm
import psutil
from functools import reduce
import math
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import multiprocessing as mp
from typing import List, Any, Optional, Callable

get_num_threads = lambda: psutil.cpu_count(logical=True)


def optional_pbar(iterator, use_pbar: bool, total_len: Optional[int] = None):
    return tqdm(iterator, total=total_len) if use_pbar else iterator


def chunk_list(
    lst: List[Any], chunk_size: int, assert_div: bool = True
) -> List[List[Any]]:
    """Split list into chunks of specified size. Raises ValueError if assert_div=True and length not divisible."""
    if assert_div and len(lst) % chunk_size != 0:
        raise ValueError(
            f"List length {len(lst)} is not divisible by chunk size {chunk_size}."
        )
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


class ParallelMap:
    def __init__(
        self,
        max_workers: Optional[int] = None,
        pbar: bool = False,
        use_thread: bool = True,
        mp_context: str = "spawn",
    ):
        self.max_workers = get_num_threads() if max_workers is None else max_workers
        self.pbar = pbar
        self.use_thread = use_thread
        self.mp_context = mp_context

    def __call__(
        self,
        fn: Callable[[Any], Any],
        args: List[Any],
        pbar: bool = True,
        on_error: Optional[Callable[[Exception, Any, int], Any]] = None,
    ) -> List[Any]:
        """Apply fn to each arg in parallel using threads or processes. Returns results in original order."""
        results = [None] * len(args)
        executor_class = ThreadPoolExecutor if self.use_thread else ProcessPoolExecutor
        executor_kwargs = {"max_workers": self.max_workers}
        if not self.use_thread:
            executor_kwargs["mp_context"] = mp.get_context(self.mp_context)

        with executor_class(**executor_kwargs) as executor:
            futures = {
                executor.submit(fn, arg): index for index, arg in enumerate(args)
            }
            for future in optional_pbar(
                as_completed(futures), self.pbar and pbar, total_len=len(args)
            ):
                index = futures[future]
                try:
                    results[index] = future.result()
                except Exception as e:
                    if on_error is not None:
                        results[index] = on_error(e, args[index], index)
                    else:
                        raise
        return results

    def chunk_apply(
        self,
        fn: Callable[[Any], Any],
        args: List[Any],
        batch_size: Optional[int] = None,
        num_chunks: Optional[int] = None,
        pbar: bool = True,
        on_error: Optional[Callable[[Exception, Any, int], Any]] = None,
    ) -> List[Any]:
        """Apply fn to args in parallel batches. Specify either batch_size or num_chunks, not both."""
        if batch_size is not None and num_chunks is not None:
            raise RuntimeError(
                "batch_size and num_chunks cannot be specified at the same time"
            )
        if num_chunks is not None:
            batch_size = math.ceil(len(args) / num_chunks)
        elif batch_size is None:
            batch_size = math.ceil(len(args) / self.max_workers)
        chunked_args = chunk_list(args, batch_size, assert_div=False)
        results = self(
            lambda args_lst: [fn(arg) for arg in args_lst],
            chunked_args,
            pbar=pbar,
            on_error=on_error,
        )
        return reduce(lambda x, y: x + y, results)
