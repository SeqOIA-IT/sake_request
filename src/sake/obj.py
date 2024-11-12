"""Define Sake dataclass."""

from __future__ import annotations

# std import
import dataclasses
import os
import pathlib
import typing

# 3rd party import
import duckdb

# project import

if typing.TYPE_CHECKING:
    import polars


DEFAULT_PATH = {
    "aggregations_path": "aggregations",
    "annotations_path": "annotations",
    "samples_path": pathlib.Path("samples") / "patients.parquet",
    "variants_path": pathlib.Path("{target}") / "variants.parquet",
}


@dataclasses.dataclass(kw_only=True)
class Sake:
    """Class that let user extract variants from sake."""

    # Mandatory member
    sake_path: pathlib.Path = dataclasses.field(kw_only=False)

    # Optional member
    threads: int | None = dataclasses.field(default=os.cpu_count())

    # Optional member generate from sake_path
    aggregations_path: pathlib.Path | None = None
    annotations_path: pathlib.Path | None = None
    samples_path: pathlib.Path | None = None
    variants_path: pathlib.Path | None = None

    # Private member
    __db: duckdb.DuckDBPyConnection = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self.__db = duckdb.connect(":memory:", config={"threads": self.threads})

        for default_field in DEFAULT_PATH:
            if self.__getattribute__(default_field) is None:
                self.__setattr__(default_field, self.sake_path / DEFAULT_PATH[default_field])

    def get_interval(self, target: str, chrom: str, start: int, stop: int) -> polars.DataFrame:
        """Get variants from chromosome between start and stop."""
        query = """
        select
            v.id, v.chr, v.pos, v.ref, v.alt
        from
            read_parquet('$path')
        where
            v.chr == '$chrom'
        and
            v.start > $start
        and
            v.stop > $stop
        """

        return self.__db.execute(
            query,
            {
                "path": str(self.variants_path).format(target=target),
                "chrom": chrom,
                "start": start,
                "stop": stop,
            },
        ).pl()


__all__: list[str] = ["Sake"]
