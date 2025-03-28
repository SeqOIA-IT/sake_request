"""Sake Request package.

A set of utils tools to interogate Seqoia dAta laKE
"""

from __future__ import annotations

# std import
import sys

# 3rd party import
# project import
from sake import _utils, utils
from sake.duckdb_query import QUERY
from sake.obj import Sake

if sys.version_info[:2] <= (3, 9):
    from sake import abc, dataclasses

__all__: list[str] = ["QUERY", "Sake", "_utils", "utils"]

if sys.version_info[:2] <= (3, 9):
    __all__ += ["abc", "dataclasses"]
