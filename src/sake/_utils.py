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

__all__ = ["QueryByGroupBy", "fix_annotation_path", "fix_variants_path", "wrap_iterator"]


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


def fix_annotation_path(
    annotations_path: pathlib.Path,
    name: str,
    version: str,
    preindication: str,
    chrom_basename: str = "1",
) -> tuple[str, bool] | None:
    """Generate annotation path by check present of file.

    Return:
      - path of annotation
      - if annotation are split by chromosome or not
    """
    path = annotations_path / name / version / preindication / f"{chrom_basename}.parquet"
    if path.is_file():
        return (str(path), True)

    path = annotations_path / name / version / f"{preindication}.parquet"
    if path.is_file():
        return (str(path), False)

    path = annotations_path / name / version / f"{chrom_basename}.parquet"
    if path.is_file():
        return (str(path), True)

    path = annotations_path / name / f"{version}.parquet"
    if path.is_file():
        return (str(path), False)

    path = annotations_path / name / f"{chrom_basename}.parquet"
    if path.is_file():
        return (str(path), True)

    return None


class QueryByGroupBy:
    """Class to run query on result of polars group by."""

    def __init__(
        self,
        threads: int,
        path_template: str,
        query_name: str,
        query_params: dict[str, str] | None = None,
        expressions: polars.IntoExpr | collections.abc.Iterable[polars.IntoExpr] | None = None,
        select_columns: list[str] | None = None,
    ):
        """Create quering object."""
        self.threads = threads
        self.path_template = path_template
        self.query_name = query_name
        self.query_params = query_params
        self.select_columns = select_columns
        self.expressions = expressions

    def __call__(self, params: tuple[tuple[int, typing.Any], polars.DataFrame]) -> polars.DataFrame | None:
        """Run query."""
        duckdb_db = duckdb.connect(":memory:")
        duckdb_db.query("SET enable_progress_bar = false;")
        duckdb_db.query(f"SET threads TO {self.threads};")

        parameter, _data = params

        path = self.path_template.format(*parameter)
        if self.query_params is not None:
            query = sake.QUERY[self.query_name].format(**self.query_params)
        else:
            query = sake.QUERY[self.query_name]

        if not os.path.isfile(path):
            return None

        result = duckdb_db.execute(
            query,
            {
                "path": path,
            },
        ).pl()

        if self.expressions is not None:
            result = result.with_columns(
                self.expressions,
            )

        if self.select_columns is not None:
            result = result.select(self.select_columns)

        return result
