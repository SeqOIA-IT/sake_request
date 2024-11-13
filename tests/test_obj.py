"""Test sake submodule."""

from __future__ import annotations

# std import
import os
import pathlib

# 3rd party import
import polars
import polars.testing

# project import
from sake import Sake

TRUTH = polars.DataFrame(
    {
        "id": [
            6409128687194603526,
            6409128687194603526,
            6409128687194603526,
            6448887235960897542,
            6448887235960897542,
            6448887235960897542,
            6450005477941051396,
            6450005477941051396,
            6450005477941051396,
            6474751846844989447,
            6474751846844989447,
            6474751846844989447,
            6482568290039234567,
            6482568290039234567,
            6490064296460943367,
            6490064296460943367,
        ],
        "chr": ["X"] * 16,
        "pos": [
            109481593,
            109481593,
            109481593,
            127995610,
            127995610,
            127995610,
            128516332,
            128516332,
            128516332,
            140039758,
            140039758,
            140039758,
            143679573,
            143679573,
            147170173,
            147170173,
        ],
        "ref": ["A", "A", "A", "C", "C", "C", "G", "G", "G", "T", "T", "T", "C", "C", "A", "A"],
        "alt": ["T", "T", "T", "T", "T", "T", "A", "A", "A", "G", "G", "G", "G", "G", "G", "G"],
        "id_part": [177, 177, 177, 178, 178, 178, 179, 179, 179, 179, 179, 179, 179, 179, 180, 180],
        "sample": [
            "CCC0",
            "CCC1",
            "CCC2",
            "DDD0",
            "DDD1",
            "DDD2",
            "DDD0",
            "DDD1",
            "DDD2",
            "AAA0",
            "AAA1",
            "AAA2",
            "AAA0",
            "AAA2",
            "AAA0",
            "AAA2",
        ],
        "gt": [2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 2],
        "ad": [
            "1,27",
            "0,43",
            "0,26",
            "0,29",
            "16,23",
            "0,22",
            "0,35",
            "0,30",
            "0,23",
            "0,45",
            "0,50",
            "0,33",
            "0,15",
            "0,51",
            "26,28",
            "0,30",
        ],
        "dp": [28, 43, 26, 29, 39, 22, 35, 30, 23, 45, 50, 33, 15, 51, 54, 30],
        "gq": [42, 99, 78, 87, 99, 66, 99, 90, 69, 99, 99, 99, 45, 99, 99, 90],
    },
    schema={
        "id": polars.UInt64,
        "chr": polars.String,
        "pos": polars.UInt64,
        "ref": polars.String,
        "alt": polars.String,
        "id_part": polars.UInt64,
        "sample": polars.String,
        "gt": polars.UInt8,
        "ad": polars.String,
        "dp": polars.UInt32,
        "gq": polars.UInt32,
    },
)


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


def test_extract_variant() -> None:
    """Check get interval."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    result = sake.get_interval("germline", "X", 47115191, 99009863)

    truth = TRUTH.drop("id_part", "sample", "gt", "ad", "dp", "gq").unique("id")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_genotype() -> None:
    """Check add genotype."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "X", 47115191, 99009863)

    result = sake.add_genotype(variants, "germline")

    truth = TRUTH

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)
