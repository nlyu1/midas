from ..dataset import ByDateDataset
from dataclasses import dataclass
from datetime import date as Date
from typing import Dict, Optional, List
import polars as pl
from pathlib import Path
from mnemosyne import DatasetType
import logging

