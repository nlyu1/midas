import torch
import torch.nn as nn
import re
import warnings
import polars as pl
from collections import OrderedDict
from typing import Dict, List
from einops import pack
from .operations import FeatureOperation
from ..utils.indexing import find_dim


def _sanitize_for_module_name(s: str) -> str:
    """Convert regex pattern to valid PyTorch module name."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', s)


class FeaturePipeline(nn.Module):
    """Apply different processor pipelines to feature groups matched by regex patterns.

    Note: feature_pack_pattern uses * to denote the unpacked feature dimension axis.
    """
    def __init__(self, processors: Dict[str, List[FeatureOperation]], features: List[str],
                 feature_pack_pattern: str = "batch *"):
        super().__init__()
        self.feature_names = features
        self.feature_pack_pattern = feature_pack_pattern
        self.feature_dim = find_dim(feature_pack_pattern, "*")
        self._parse_feature_groups(OrderedDict(processors))

    def _parse_feature_groups(self, processors: OrderedDict[str, List[FeatureOperation]],
                              warn_noncontiguous: bool = True, warn_duplicates: bool = True):
        """Match features to regex patterns and build processor modules."""
        self.regex_patterns = list(processors.keys())
        sanitized_names = [_sanitize_for_module_name(s) for s in self.regex_patterns]

        self.module_dict = nn.ModuleDict({
            name: nn.Sequential(*ops) for name, ops in zip(sanitized_names, processors.values())
        })

        remaining = set(self.feature_names)
        for k, (regex_str, sanitized) in enumerate(zip(self.regex_patterns, sanitized_names)):
            regex = re.compile(regex_str)
            matches = [(j, f) for j, f in enumerate(self.feature_names) if f in remaining and regex.fullmatch(f)]
            matched_names = {f for _, f in matches}
            remaining -= matched_names

            all_matches = [f for f in self.feature_names if regex.fullmatch(f)]
            duplicates = set(all_matches) - matched_names
            if duplicates and warn_duplicates:
                warnings.warn(f"Regex '{regex_str}' matches {len(duplicates)} already-matched features: {duplicates}")

            indices = sorted([j for j, _ in matches])
            if indices and warn_noncontiguous:
                if len(indices) != max(indices) - min(indices) + 1:
                    warnings.warn(f"Features matched by '{regex_str}' are not contiguous: {[self.feature_names[i] for i in indices]}")

            self.register_buffer(f'_group_{k}_indices', torch.LongTensor(indices))

        if remaining:
            raise RuntimeError(f'Unmatched features: {remaining}')

        self.group_names = sanitized_names

    def fit(self, x: torch.Tensor) -> torch.Tensor:
        """Fit all processors and return transformed data."""
        groups = [torch.index_select(x, self.feature_dim, getattr(self, f'_group_{k}_indices'))
                  for k in range(len(self.module_dict))]
                  
        transformed = []
        for x_group, name in zip(groups, self.group_names):
            for op in self.module_dict[name]:
                op.fit(x_group)
                x_group = op(x_group)
            transformed.append(x_group)

        return pack(transformed, self.feature_pack_pattern)[0]

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Apply fitted processors to data."""
        groups = [torch.index_select(x, self.feature_dim, getattr(self, f'_group_{k}_indices'))
                  for k in range(len(self.module_dict))]

        transformed = []
        for x_group, name in zip(groups, self.group_names):
            for op in self.module_dict[name]:
                x_group = op(x_group)
            transformed.append(x_group)

        return pack(transformed, self.feature_pack_pattern)[0]

    def match_info(self) -> pl.DataFrame:
        """Return DataFrame showing feature-to-regex mappings, sorted by feature_index."""
        records = [
            {
                "feature": self.feature_names[idx],
                "feature_index": idx,
                "regex_match": regex_str,
                "regex_match_index": group_idx,
            }
            for group_idx, regex_str in enumerate(self.regex_patterns)
            for idx in getattr(self, f'_group_{group_idx}_indices').tolist()
        ]
        return pl.DataFrame(records).sort("feature_index")

    def fit_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fit processors on DataFrame and return result with replaced feature columns."""
        if self.feature_dim != 1:
            raise ValueError("DataFrame operations require feature_dim=1")
        x = torch.from_numpy(df.select(pl.col(self.feature_names)).to_numpy()).float()
        x_transformed = self.fit(x)
        n_out = x_transformed.shape[1]
        out_features = self.feature_names if n_out == len(self.feature_names) else [f"feature_{i}" for i in range(n_out)]
        return df.with_columns([
            pl.Series(name, x_transformed[:, i].numpy()) for i, name in enumerate(out_features)
        ])

    def transform_df(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply fitted processors to DataFrame and return result with replaced feature columns."""
        if self.feature_dim != 1:
            raise ValueError("DataFrame operations require feature_dim=1")
        x = torch.from_numpy(df.select(pl.col(self.feature_names)).to_numpy()).float()
        x_transformed = self.forward(x)
        n_out = x_transformed.shape[1]
        out_features = self.feature_names if n_out == len(self.feature_names) else [f"feature_{i}" for i in range(n_out)]
        return df.with_columns([
            pl.Series(name, x_transformed[:, i].numpy()) for i, name in enumerate(out_features)
        ])
