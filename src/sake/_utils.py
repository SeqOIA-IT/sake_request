"""Define internal utils function."""

from __future__ import annotations

# std import
import os
import typing

# 3rd party import
import duckdb
from tqdm.auto import tqdm

# project import
import sake

if typing.TYPE_CHECKING:  # pragma: no cover
    # std import
    import collections
    import pathlib

    import polars

__all__ = ["QueryByGroupBy", "fix_annotation_version", "fix_variants_path", "wrap_iterator"]


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
        return str(path / "*.parquet")
    if path.is_dir():
        return str(path / f"{chrom}.parquet")
    return str(path.with_suffix(".parquet"))


def fix_annotation_version(name: str, version: str, preindication: str) -> str:
    if name in {"snpeff", "variant2gene"}:
        return f"{version}/{preindication}"
    if name == "spliceai":
        return ""

    return version


class QueryByGroupBy:
    """Class to run query on result of polars group by."""

    def __init__(
        self,
        threads: int,
        path_template: str,
        query_name: str,
        expressions: polars.IntoExpr | collections.abc.Iterable[polars.IntoExpr] | None = None,
        drop_column: list[str] | None = None,
    ):
        """Create quering object."""
        self.threads = threads
        self.path_template = path_template
        self.query_name = query_name
        self.drop_column = drop_column
        self.expressions = expressions

    def __call__(self, params: tuple[tuple[int, typing.Any], polars.DataFrame]) -> polars.DataFrame | None:
        """Run query."""
        duckdb_db = duckdb.connect(":memory:")
        duckdb_db.query("SET enable_progress_bar = false;")
        duckdb_db.query(f"SET threads TO {self.threads};")

        parameter, _data = params

        path = self.path_template.format(*parameter)

        if not os.path.isfile(path):
            return None

        result = duckdb_db.execute(
            sake.QUERY[self.query_name],
            {
                "path": path,
            },
        ).pl()

        if self.expressions is not None:
            result = result.with_columns(
                self.expressions,
            )

        if self.drop_column is not None:
            result = result.drop(self.drop_column)

        return result
