"""Define internal utils function."""

from __future__ import annotations

# std import
import typing

# 3rd party import
import duckdb
import polars
from tqdm.auto import tqdm

# project import
import sake

if typing.TYPE_CHECKING:
    # std import
    import collections
    import pathlib

__all__ = ["GenotypeQuery", "fix_annotation_version", "fix_variants_path", "wrap_iterator"]


def wrap_iterator(
    activate_tqdm: bool,  # noqa: FBT001
    iterator: collections.abc.Iterable[typing.Any],
    *,
    total: int | None = None,
) -> collections.abc.Iterable[typing.Any]:
    """Wrap iterator on tqdm or not."""
    if activate_tqdm:
        if total is None:
            return tqdm(iterator)
        return tqdm(iterator, total=total)
    return iterator


def fix_variants_path(path: pathlib.Path, chrom: str | None = None) -> str:
    """Fix variants path to match if variants are split or not."""
    if chrom is None and path.is_dir():
        return str(path / "*.parquett")
    if path.is_dir():
        return str(path / f"{chrom}.parquett")
    return str(path.with_suffix(".parquet"))


def fix_annotation_version(name: str, version: str, preindication: str) -> str:
    if name in {"snpeff", "variant2gene"}:
        return f"{version}/{preindication}"
    if name == "spliceai":
        return ""

    return version


class GenotypeQuery:
    """Class to run genotype quering."""

    def __init__(self, threads: int, prefix: pathlib.Path, drop_column: list[str]):
        """Create genotyping query object."""
        self.threads = threads
        self.prefix = prefix
        self.drop_column = drop_column

    def __call__(self, params: tuple[tuple[int, typing.Any], polars.DataFrame]) -> polars.DataFrame | None:
        """Run genotyping of data with information in path."""
        duckdb_db = duckdb.connect(":memory:")
        duckdb_db.query("SET enable_progress_bar = false;")
        duckdb_db.query(f"SET threads TO {self.threads};")

        (id_part, *_), _data = params

        part_path = self.prefix / f"id_part={id_part}/0.parquet"
        if not part_path.is_file():
            return None

        return (
            duckdb_db.execute(
                sake.QUERY["genotype_query"],
                {
                    "path": str(part_path),
                },
            )
            .pl()
            .with_columns(
                ad=polars.col("ad").cast(polars.List(polars.String)).list.join(","),
            )
            .drop(self.drop_column)
        )
