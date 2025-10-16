import torch
from typing import Callable, Any
from einops import parse_shape, rearrange, reduce


def _printv(msg: str, verbose: bool) -> None:
    """Print if verbose is True."""
    if verbose:
        print(msg)

def nanstd(o: torch.Tensor, dim: int, keepdim: bool = False) -> torch.Tensor:
    """
    Compute standard deviation ignoring NaN values.

    This is a helper function that computes the standard deviation along a specified
    dimension while ignoring NaN values. It uses nanmean for both the mean and
    variance calculations.

    Args:
        o: Input tensor.
        dim: Dimension along which to compute standard deviation.
        keepdim: If True, retain the reduced dimension with size 1.

    Returns:
        Tensor containing standard deviations with NaNs ignored.
    """
    result = torch.sqrt(
        torch.nanmean(
            torch.pow(torch.abs(o - torch.nanmean(o, dim=dim).unsqueeze(dim)), 2),
            dim=dim,
        )
    )
    if keepdim:
        result = result.unsqueeze(dim)
    return result

def multidim_reduce_keepdim(
    fn: Callable[[Any, int, bool], Any],
    input_shape: str,
    output_shape: str,
    verbose_debug: bool = False
) -> Callable[[Any], Any]:
    """
    Broadcasts a single-dim reduce function to multiple dimensions.
    
    Examples:
        >>> multidim_reduce_keepdim(torch.nanmean, 'a b c d', '() () () d')(data).shape  # [2,2,4,2] -> [1,1,1,2]
        >>> multidim_reduce_keepdim(torch.nanmean, 'a b c d', 'a () () d')(data).shape  # [2,2,4,2] -> [2,1,1,2]
    """
    
    def reduce_helper(x: Any, reduce_axes: tuple[int, ...]) -> Any:
        max_axis_index = max(reduce_axes)
        all_axis_names = [f'x{i}' for i in range(max_axis_index + 1)]
        kept_axis_names = [f'x{i}' for i in range(max_axis_index + 1) if i not in reduce_axes]
        reduced_axis_names = [f'x{i}' for i in range(max_axis_index + 1) if i in reduce_axes]
        
        source_pattern = ' '.join(all_axis_names) + ' ... '
        kept_axes_pattern = ' '.join(kept_axis_names) + ' ... '
        combined_pattern = f"({' '.join(reduced_axis_names)}) {kept_axes_pattern}"
        
        _printv(f'{x.shape} | axes={reduce_axes}', verbose_debug)  # [2,3,4,5] | axes=(1,2)
        _printv(f'{source_pattern} -> {combined_pattern}', verbose_debug)  # x0 x1 x2 x3 ... -> (x1 x2) x0 x3 ...
        
        axes_combined = rearrange(x, f'{source_pattern} -> {combined_pattern}')
        _printv(f'Combined: {axes_combined.shape}', verbose_debug)  # [12,2,5]
        
        reduced_result = fn(axes_combined, dim=0, keepdim=True)
        _printv(f'Reduced: {reduced_result.shape}', verbose_debug)  # [1,2,5]
        
        return reduced_result
    
    return lambda x: reduce(x, f'{input_shape} -> {output_shape}', reduce_helper)


def multidim_apply_keepdim(
    fn: Callable[[Any, int], Any],
    input_shape: str,
    output_shape: str,
    verbose_debug: bool = False
) -> Callable[[Any], Any]:
    """
    Broadcasts a single-dim shape-preserving function to multiple dimensions.
    
    Examples:
        >>> multidim_apply_keepdim(F.softmax, 'a b c d', '() () () d')(data).shape  # [2,3,4,5] -> [2,3,4,5]
        >>> multidim_apply_keepdim(F.softmax, 'a b c d', 'a () () d')(data).shape  # [2,3,4,5] -> [2,3,4,5]
    """
    
    input_axes = input_shape.split()
    output_axes = output_shape.split()
    
    apply_axis_names = [inp for inp, out in zip(input_axes, output_axes) if out == '()']
    kept_axis_names = [inp for inp, out in zip(input_axes, output_axes) if out != '()']
    
    combined_axis = f"({' '.join(apply_axis_names)})" if apply_axis_names else "()"
    temp_pattern = f"{combined_axis} {' '.join(kept_axis_names)}".strip()
    
    def apply_fn(x: Any) -> Any:
        _printv(f'{x.shape}', verbose_debug)  # [2,3,4,5]
        _printv(f'{input_shape} -> {temp_pattern}', verbose_debug)  # a b c d -> (b c) a d
        
        shapes = parse_shape(x, input_shape)
        reshaped = rearrange(x, f'{input_shape} -> {temp_pattern}')
        _printv(f'Combined: {reshaped.shape}', verbose_debug)  # [12,2,5]
        
        result = fn(reshaped, dim=0)
        _printv(f'Applied: {result.shape}', verbose_debug)  # [12,2,5]
        
        final = rearrange(result, f'{temp_pattern} -> {input_shape}', **shapes)
        _printv(f'Final: {final.shape}', verbose_debug)  # [2,3,4,5]
        
        return final
    
    return apply_fn