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


def test_chromosome_path() -> None:
    """Check fix variants path."""
    assert set(sake._utils.get_chromosome_path(pathlib.Path("tests/data/germline/variants"))) == {
        pathlib.Path(f"tests/data/germline/variants/{chrom}.parquet") for chrom in [*range(1, 23), "X", "Y"]
    }
    assert set(
        sake._utils.get_chromosome_path(
            pathlib.Path("tests/data/germline/variants"),
            [f"{chrom}" for chrom in range(1, 11)],
        ),
    ) == {pathlib.Path(f"tests/data/germline/variants/{chrom}.parquet") for chrom in range(1, 11)}


def test_fix_annotation_path() -> None:
    """Check fix annotation version."""
    path = pathlib.Path("tests/data/annotations")

    result = sake._utils.fix_annotation_path(path, "snpeff", "4.3t", "germline")
    assert result is not None
    assert result[0] == path / "snpeff" / "4.3t" / "germline" / "1.parquet"
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "snpeff", "4.3t", "germline", chrom_basename="X")
    assert result is not None
    assert result[0] == path / "snpeff" / "4.3t" / "germline" / "X.parquet"
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "nvp", "1.0", "germline")
    assert result is not None
    assert result[0] == path / "nvp" / "1.0" / "germline.parquet"
    assert not result[1]

    result = sake._utils.fix_annotation_path(path, "gnomad", "3.1.2", "germline")
    assert result is not None
    assert result[0] == path / "gnomad" / "3.1.2" / "1.parquet"
    assert result[1]

    result = sake._utils.fix_annotation_path(path, "nc", "1.0", "germline")
    assert result is not None
    assert result[0] == path / "nc" / "1.parquet"
    assert result[1]
