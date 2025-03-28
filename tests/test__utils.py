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


def test_fix_annotation_version() -> None:
    """Check fix annotation version."""
    assert sake._utils.fix_annotation_version("snpeff", "aaa", "bbb") == "aaa/bbb"
    assert sake._utils.fix_annotation_version("variant2gene", "aaa", "bbb") == "aaa/bbb"
    assert sake._utils.fix_annotation_version("spliceai", "aaa", "bbb") == ""
    assert sake._utils.fix_annotation_version("gnomad", "aaa", "bbb") == "aaa"
