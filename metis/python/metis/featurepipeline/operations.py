import torch
import torch.nn as nn
from abc import ABC, abstractmethod
import math
from ..utils.torchops import multidim_reduce_keepdim
from ..utils.indexing import build_conj_shape
from einops import parse_shape, rearrange
from typing import Literal, Union, Any, Callable
from functools import cached_property


class ConjShapePair:
    """Rearrange tensor to conjugated shape, apply operation, rearrange back."""

    def __init__(
        self,
        input_shape: str,
        conj_shape: str,
        assert_conj_dim: Union[int, None] = None,
    ):
        self.input_shape = input_shape
        self.conj_shape = conj_shape
        self.assert_conj_dim = assert_conj_dim

    def to_conj(self, x: torch.Tensor) -> torch.Tensor:
        self.input_shape_dict = parse_shape(x, self.input_shape)
        conj = rearrange(x, f"{self.input_shape} -> {self.conj_shape}")
        if self.assert_conj_dim is not None and conj.dim() != self.assert_conj_dim:
            raise RuntimeError(
                f"Expected {self.assert_conj_dim} dims, got {conj.dim()}"
            )
        return conj

    def from_conj(self, conj: torch.Tensor) -> torch.Tensor:
        return rearrange(
            conj, f"{self.conj_shape} -> {self.input_shape}", **self.input_shape_dict
        )

    def conj_apply(
        self, x: torch.Tensor, fn: Callable[[torch.Tensor], torch.Tensor]
    ) -> torch.Tensor:
        input_shape_dict = parse_shape(x, self.input_shape)
        conj = rearrange(x, f"{self.input_shape} -> {self.conj_shape}")
        if self.assert_conj_dim is not None and conj.dim() != self.assert_conj_dim:
            raise RuntimeError(
                f"Expected {self.assert_conj_dim} dims, got {conj.dim()}"
            )
        result = fn(conj)
        return rearrange(
            result, f"{self.conj_shape} -> {self.input_shape}", **input_shape_dict
        )


class FeatureOperation(nn.Module, ABC):
    """Base class for stateful feature transformations."""

    def __init__(self, input_shape: str = "batch feature"):
        super().__init__()
        if "..." in input_shape:
            raise ValueError(f"input_shape must be fully specified, got: {input_shape}")
        self.is_fitted: bool = False
        self.input_shape = input_shape

    @cached_property
    def device(self) -> Union[torch.device, None]:
        try:
            return next(self.buffers()).device
        except StopIteration:
            return None

    @abstractmethod
    def _fit(self, tensor: torch.Tensor) -> None:
        pass

    @abstractmethod
    def _forward(self, tensor: torch.Tensor) -> Any:
        pass

    def check_input(self, tensor: torch.Tensor):
        try:
            parse_shape(tensor, self.input_shape)
        except Exception as e:
            raise RuntimeError(f"Expected {self.input_shape}, got {tensor.shape}: {e}")

    def forward(self, tensor):
        self.check_input(tensor)
        if not self.is_fitted:
            raise RuntimeError(f"Please fit {self} before forward pass")
        if (self.device is not None) and (self.device != tensor.device):
            raise RuntimeError(f"Device mismatch: {self.device} vs {tensor.device}")
        return self._forward(tensor)

    def fit(self, tensor: torch.Tensor, refit: bool = False) -> "FeatureOperation":
        self.check_input(tensor)
        if self.is_fitted and not refit:
            raise RuntimeError("Already fitted. Set refit=True to override.")
        self._fit(tensor)
        self.to(tensor.device)
        self.is_fitted = True
        return self


class Identity(FeatureOperation):
    """No-op pass-through operation."""

    def _fit(self, x: torch.Tensor) -> None:
        pass

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return x


class FillNans(FeatureOperation):
    """Fill NaN values with median or constant."""

    def __init__(
        self,
        fill_value: Union[float, Literal["median"]] = "median",
        reduce_shape="() feature",
        **kwargs,
    ):
        super().__init__(**kwargs)
        if not (fill_value == "median" or isinstance(fill_value, (int, float))):
            raise ValueError("`fill_value` must be 'median' or a number.")
        self.fill_value = fill_value
        self.register_buffer("fill_tensor", torch.tensor(0.0))
        self.reduce_shape = reduce_shape

    def _fit(self, x: torch.Tensor) -> None:
        if self.fill_value == "median":
            median_fn = multidim_reduce_keepdim(
                torch.nanmedian, self.input_shape, self.reduce_shape
            )
            median_val = median_fn(x)
            if torch.isnan(median_val).any():
                raise RuntimeError("Input contains only NaN values")
            self.register_buffer("fill_tensor", median_val)
        else:
            self.register_buffer("fill_tensor", torch.tensor(self.fill_value))

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.where(torch.isnan(x), self.fill_tensor, x)


class Std(FeatureOperation):
    """Standardize by subtracting mean and dividing by std."""

    def __init__(
        self,
        normalize_shape: str = "() feature",
        mean: Union[float, None] = None,
        std: Union[float, None] = None,
        ignore_nan_during_fit: bool = True,
        min_eps: float = 1e-5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.normalize_shape = normalize_shape
        self.mean = mean
        self.std = std
        self.min_eps = min_eps
        self.ignore_nans = ignore_nan_during_fit

    def _fit(self, x: torch.Tensor):
        if not self.ignore_nans and torch.isnan(x).any():
            raise RuntimeError("ignore_nan_during_fit=False but encountered NaN")
        if self.mean is not None:
            mean_tensor = torch.tensor(self.mean).type_as(x)
        else:
            mean_fn = multidim_reduce_keepdim(
                torch.nanmean, self.input_shape, self.normalize_shape
            )
            mean_tensor = mean_fn(x)
        if self.std is not None:
            std_tensor = torch.tensor(self.std).type_as(x)
        else:
            std_fn = multidim_reduce_keepdim(
                nanstd, self.input_shape, self.normalize_shape
            )
            std_tensor = std_fn(x)
        self.register_buffer("mean_tensor", mean_tensor)
        self.register_buffer("std_tensor", std_tensor)
        if self.std_tensor.abs().min() < self.min_eps:
            raise RuntimeError(
                f"Min std too small: {self.std_tensor.abs().min().item()}"
            )

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return (x - self.mean_tensor) / self.std_tensor


class Clip(FeatureOperation):
    """Clip values to [min, max] range."""

    def __init__(
        self,
        min_value: Union[float, None] = None,
        max_value: Union[float, None] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if min_value is None and max_value is None:
            raise ValueError("At least one of min_value or max_value required")
        self.min_value = min_value
        self.max_value = max_value

    def _fit(self, x: torch.Tensor) -> None:
        pass

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.clip(x, min=self.min_value, max=self.max_value)


class ClipQuantiles(FeatureOperation):
    """Clip values based on learned quantiles."""

    def __init__(
        self,
        normalize_shape: str = "() feature",
        min_quantile: Union[float, None] = None,
        max_quantile: Union[float, None] = None,
        ignore_nans_during_fit: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if min_quantile is None and max_quantile is None:
            raise ValueError("At least one of min_quantile or max_quantile required")
        if min_quantile is not None and not (0 <= min_quantile <= 1):
            raise ValueError("min_quantile must be in [0, 1]")
        if max_quantile is not None and not (0 <= max_quantile <= 1):
            raise ValueError("max_quantile must be in [0, 1]")
        if (
            min_quantile is not None
            and max_quantile is not None
            and min_quantile >= max_quantile
        ):
            raise ValueError("min_quantile must be < max_quantile")
        self.normalize_shape = normalize_shape
        self.min_quantile = min_quantile
        self.max_quantile = max_quantile
        self.ignore_nans = ignore_nans_during_fit

    def _fit(self, x: torch.Tensor) -> None:
        if not self.ignore_nans and torch.isnan(x).any():
            raise RuntimeError("ignore_nans_during_fit=False but encountered NaN")
        if self.min_quantile is not None:
            min_fn = multidim_reduce_keepdim(
                lambda t, dim, keepdim: torch.nanquantile(
                    t, self.min_quantile, dim=dim, keepdim=keepdim
                ),
                self.input_shape,
                self.normalize_shape,
            )
            self.register_buffer("min_tensor", min_fn(x))
        if self.max_quantile is not None:
            max_fn = multidim_reduce_keepdim(
                lambda t, dim, keepdim: torch.nanquantile(
                    t, self.max_quantile, dim=dim, keepdim=keepdim
                ),
                self.input_shape,
                self.normalize_shape,
            )
            self.register_buffer("max_tensor", max_fn(x))

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        min_val = self.min_tensor if self.min_quantile is not None else None
        max_val = self.max_tensor if self.max_quantile is not None else None
        return torch.clip(x, min=min_val, max=max_val)


class ElementwiseOp(FeatureOperation):
    """Apply arbitrary element-wise function."""

    def __init__(self, fn: Callable[[torch.Tensor], torch.Tensor], **kwargs):
        super().__init__(**kwargs)
        self.fn = fn

    def _fit(self, x: torch.Tensor) -> None:
        pass

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.fn(x)


class Log(FeatureOperation):
    """Apply log(x + epsilon)."""

    def __init__(self, epsilon: float = 1e-8, **kwargs):
        super().__init__(**kwargs)
        self.epsilon = epsilon

    def _fit(self, x: torch.Tensor) -> None:
        pass

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.log(x + self.epsilon)


class NonfiniteRaise(FeatureOperation):
    """Validation: raise on NaN/Inf values."""

    def __init__(self, on_nans: bool = True, on_inf: bool = True, **kwargs):
        super().__init__(**kwargs)
        if not on_nans and not on_inf:
            raise ValueError("At least one of on_nans or on_inf must be True")
        self.on_nans = on_nans
        self.on_inf = on_inf

    def _fit(self, x: torch.Tensor) -> None:
        pass

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        issues = []
        if self.on_nans and (nan_count := torch.isnan(x).sum().item()) > 0:
            issues.append(f"{nan_count} NaN")
        if self.on_inf:
            if (posinf := torch.isposinf(x).sum().item()) > 0:
                issues.append(f"{posinf} +inf")
            if (neginf := torch.isneginf(x).sum().item()) > 0:
                issues.append(f"{neginf} -inf")
        if issues:
            raise RuntimeError(f"Non-finite values: {', '.join(issues)}/{x.numel()}")
        return x


class ToQuantile(FeatureOperation):
    """Transform to quantile space (uniform [0,1] or Gaussian). Uses vectorized binary search."""

    def __init__(
        self,
        normalize_shape: str = "() feature",
        method: Literal["uniform", "gaussian"] = "uniform",
        n_quantiles: int = 1000,
        ignore_nans_during_fit: bool = True,
        output_clip: bool = True,
        subsample: Union[int, None] = None,
        input_shape: str = "batch feature",
        **kwargs,
    ):
        super().__init__(input_shape=input_shape, **kwargs)
        if method not in ["uniform", "gaussian"]:
            raise ValueError("method must be 'uniform' or 'gaussian'")
        conj_shape = build_conj_shape(input_shape, normalize_shape)
        self.cspair = ConjShapePair(input_shape, conj_shape, assert_conj_dim=2)
        self.method = method
        self.n_quantiles = n_quantiles
        self.ignore_nans = ignore_nans_during_fit
        self.output_clip = output_clip
        self.subsample = subsample

    def _fit(self, x: torch.Tensor) -> None:
        x_rearranged = self.cspair.to_conj(x)
        if not self.ignore_nans and torch.isnan(x).any():
            raise RuntimeError("ignore_nans_during_fit=False but encountered NaN")
        _, n_samples = x_rearranged.shape
        if self.subsample is not None and n_samples > self.subsample:
            indices = torch.randperm(n_samples, device=x.device)[: self.subsample]
            x_rearranged = x_rearranged[:, indices]
        q_positions = torch.linspace(
            0, 1, self.n_quantiles, device=x.device, dtype=x.dtype
        )
        quantiles = torch.nanquantile(x_rearranged, q_positions, dim=1).T
        self.register_buffer("quantiles", quantiles)
        self.register_buffer("q_positions", q_positions)

    def _forward(self, x: torch.Tensor) -> torch.Tensor:
        def transform_fn(y: torch.Tensor) -> torch.Tensor:
            # Vectorized binary search + linear interpolation
            indices = torch.searchsorted(self.quantiles, y, right=False)
            indices = torch.clamp(indices, 1, self.n_quantiles - 1)
            feat_idx = (
                torch.arange(y.shape[0], device=y.device)
                .unsqueeze(1)
                .expand_as(indices)
            )
            q_lo = self.quantiles[feat_idx, indices - 1]
            q_hi = self.quantiles[feat_idx, indices]
            pos_lo = self.q_positions[indices - 1]
            pos_hi = self.q_positions[indices]
            denom = q_hi - q_lo
            safe_denom = torch.where(denom.abs() < 1e-10, torch.ones_like(denom), denom)
            frac = torch.clamp((y - q_lo) / safe_denom, 0.0, 1.0)
            quantile_pos = pos_lo + frac * (pos_hi - pos_lo)
            if self.method == "uniform":
                result = quantile_pos
                if self.output_clip:
                    result = torch.clamp(result, 0.0, 1.0)
            elif self.method == "gaussian":
                if self.output_clip:
                    quantile_pos = torch.clamp(quantile_pos, 1e-7, 1 - 1e-7)
                result = math.sqrt(2) * torch.erfinv(2 * quantile_pos - 1)
                if self.output_clip:
                    result = torch.clamp(result, -8.0, 8.0)
            return result

        return self.cspair.conj_apply(x, transform_fn)
