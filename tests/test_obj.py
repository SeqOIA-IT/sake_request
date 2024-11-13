"""Test sake submodule."""

from __future__ import annotations

# std import
import os
import pathlib

# 3rd party import
# project import
from sake import Sake


def test_default_value() -> None:
    """Check default object."""
    sake_path = pathlib.Path("sake")

    database = Sake(sake_path)

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(os.cpu_count())

    assert database.aggregations_path == sake_path / "aggregations"
    assert database.annotations_path == sake_path / "annotations"
    assert database.partitions_path == sake_path / "{target}" / "genotypes" / "partitions"
    assert database.samples_path == sake_path / "samples" / "patients.parquet"
    assert database.transmissions_path == sake_path / "{target}" / "genotypes" / "transmissions"
    assert database.variants_path == sake_path / "{target}" / "variants.parquet"


def test_set_value() -> None:
    """Check default object."""
    sake_path = pathlib.Path("sake")

    database = Sake(
        sake_path,
        threads=23,
        aggregations_path=sake_path / "other",
        annotations_path=sake_path / "other",
        partitions_path=sake_path / "other",
        samples_path=sake_path / "other",
        transmissions_path=sake_path / "other",
        variants_path=sake_path / "other",
    )

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(23)

    assert database.aggregations_path == sake_path / "other"
    assert database.annotations_path == sake_path / "other"
    assert database.partitions_path == sake_path / "other"
    assert database.samples_path == sake_path / "other"
    assert database.transmissions_path == sake_path / "other"
    assert database.variants_path == sake_path / "other"
