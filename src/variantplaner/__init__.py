"""Variantplaner Request package.

A set of utils tools to interogate Seqoia dAta laKE
"""

from __future__ import annotations

# std import
import sys

from variantplaner import utils

# 3rd party import
# project import
from variantplaner.obj import Variantplaner

if sys.version_info[:2] <= (3, 9):
    from variantplaner import abc, dataclasses

__all__: list[str] = ["Variantplaner", "utils"]

if sys.version_info[:2] <= (3, 9):
    __all__ += ["dataclasses", "abc"]
