from typing import Tuple, List


def parse_einops_axes(input_shape: str, output_shape: str) -> Tuple[List[str], List[str]]:
    """Extract kept and reduced axes from input/output shape pairs.

    Args:
        input_shape: Input einops pattern (e.g., 'a b c d')
        output_shape: Output einops pattern (e.g., 'a () () d')

    Returns:
        (kept_axes, reduced_axes) tuple of axis names
    """
    input_axes = input_shape.split()
    output_axes = output_shape.split()
    if len(input_axes) != len(output_axes):
        raise ValueError(f"Shapes must have same length: {input_shape} vs {output_shape}")
    kept = [inp for inp, out in zip(input_axes, output_axes) if out != '()']
    reduced = [inp for inp, out in zip(input_axes, output_axes) if out == '()']
    return kept, reduced


def find_dim(pattern: str, axis_name: str) -> int:
    """Find dimension index of axis in einops pattern, supporting negative indexing.

    Args:
        pattern: Einops pattern like 'batch feature', '* feature', or '... last_2 last_1'
        axis_name: Name of the axis to find

    Returns:
        Dimension index (positive or negative)

    Examples:
        >>> find_dim('batch feature', 'feature')
        1
        >>> find_dim('* feature', 'feature')
        -1
        >>> find_dim('... last_2 last_1', 'last_1')
        -1
        >>> find_dim('... last_2 last_1', 'last_2')
        -2
    """
    tokens = pattern.split()
    if axis_name not in tokens:
        raise ValueError(f"Axis '{axis_name}' not found in pattern '{pattern}'")

    ellipsis_idx = -1
    for i, t in enumerate(tokens):
        if t in ('...', '*'):
            ellipsis_idx = i
            break

    if ellipsis_idx >= 0:
        after_tokens = [t for t in tokens[ellipsis_idx+1:] if t not in ('(', ')')]
        if axis_name in after_tokens:
            pos_in_after = after_tokens.index(axis_name)
            return -(len(after_tokens) - pos_in_after)

    # No ellipsis, or axis before ellipsis - use positive indexing
    count = 0
    for t in tokens:
        if t == axis_name:
            return count
        if t not in ('(', ')', '...', '*'):
            count += 1

    raise ValueError(f"Could not determine dimension for '{axis_name}' in '{pattern}'")


def build_conj_shape(input_shape: str, normalize_shape: str) -> str:
    """Build conjugated shape pattern for 2D operations.

    Converts input/output shape pair into a 2D rearrangement pattern where
    kept dimensions are combined into axis 0, reduced dimensions into axis 1.

    Args:
        input_shape: Input einops pattern
        normalize_shape: Output pattern with () for reduced dims

    Returns:
        Conjugated shape pattern like '(kept_axes) (reduced_axes)'

    Examples:
        >>> build_conj_shape('batch feature', '() feature')
        '(feature) (batch)'  # [n_features, batch_size] for per-feature ops across batch
        >>> build_conj_shape('batch feature', 'batch ()')
        '(batch) (feature)'  # [batch_size, n_features] for per-batch ops across features
        >>> build_conj_shape('seq batch feature', '() () feature')
        '(feature) (seq batch)'  # [n_features, seq*batch] for per-feature ops
    """
    kept, reduced = parse_einops_axes(input_shape, normalize_shape)
    if not reduced:
        raise ValueError("normalize_shape must have at least one reduced dimension ()")
    kept_part = f"({' '.join(kept)})" if kept else "1"
    reduced_part = f"({' '.join(reduced)})"
    return f"{kept_part} {reduced_part}".strip()
