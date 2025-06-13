"""Sake Request package.

A set of utils tools to interogate Seqoia dAta laKE
"""

from __future__ import annotations

# 3rd party import
# project import
from sake import _utils, utils
from sake.duckdb_query import QUERY
from sake.obj import Sake

__all__: list[str] = ["QUERY", "Sake", "_utils", "utils"]

__version__ = "0.3.0"
