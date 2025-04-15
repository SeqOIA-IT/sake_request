"""Test _utils submodule."""

from __future__ import annotations

# std import
import pathlib

# 3rd party import
from tqdm.auto import tqdm

# project import
import sake


def test_wrap_iterator() -> None:
    """Check fix wrap iterator."""
    assert sake._utils.wrap_iterator(False, range(10), total=10) == range(10)  # noqa: FBT003
    assert sake._utils.wrap_iterator(True, range(10)).iterable == tqdm(range(10)).iterable  # type: ignore[attr-defined] # noqa: FBT003
    assert sake._utils.wrap_iterator(True, range(10)).total == tqdm(range(10)).total  # type: ignore[attr-defined] # noqa: FBT003
    assert sake._utils.wrap_iterator(True, range(10), total=10).iterable == tqdm(range(10), total=10).iterable  # type: ignore[attr-defined] # noqa: FBT003
    assert sake._utils.wrap_iterator(True, range(10), total=10).total == tqdm(range(10), total=10).total  # type: ignore[attr-defined] # noqa: FBT003


def test_variants_path() -> None:
    """Check fix variants path."""
    assert sake._utils.fix_variants_path(pathlib.Path("test")) == "test.parquet"
    assert sake._utils.fix_variants_path(pathlib.Path("tests/data/germline")) == "tests/data/germline/*.parquet"
    assert sake._utils.fix_variants_path(pathlib.Path("tests/data/germline"), "10") == "tests/data/germline/10.parquet"


def test_fix_annotation_path() -> None:
    """Check fix annotation version."""
    path = pathlib.Path("tests/data/annotations")

    result = sake._utils.fix_annotation_path(path, "snpeff", "4.3t", "germline")
    assert result is not None
    assert result[0] == str(path / "snpeff" / "4.3t" / "germline" / "1.parquet")
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "snpeff", "4.3t", "germline", chrom_basename="X")
    assert result is not None
    assert result[0] == str(path / "snpeff" / "4.3t" / "germline" / "X.parquet")
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "nvp", "1.0", "germline")
    assert result is not None
    assert result[0] == str(path / "nvp" / "1.0" / "germline.parquet")
    assert not result[1]

    result = sake._utils.fix_annotation_path(path, "gnomad", "3.1.2", "germline")
    assert result is not None
    assert result[0] == str(path / "gnomad" / "3.1.2" / "1.parquet")
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "nc", "1.0", "germline")
    assert result is not None
    assert result[0] == str(path / "nc" / "1.parquet")
    assert result[1]
