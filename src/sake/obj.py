"""Define Sake dataclass."""

from __future__ import annotations

# std import
import dataclasses
import os
import pathlib

# 3rd party import
import duckdb
import polars

# project import
import sake

DEFAULT_PATH = {
    "aggregations_path": "aggregations",
    "annotations_path": "annotations",
    "samples_path": pathlib.Path("samples") / "patients.parquet",
    "variants_path": pathlib.Path("{target}") / "variants.parquet",
    "partitions_path": pathlib.Path("{target}") / "genotypes" / "partitions",
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
    partitions_path: pathlib.Path | None = None

    # Private member
    __db: duckdb.DuckDBPyConnection = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self.__db = duckdb.connect(":memory:", config={"threads": self.threads})
        os.environ["POLARS_MAX_THREADS"] = self.threads

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

    def add_genotype(
        self,
        variants: polars.DataFrame,
        target: str,
        *,
        keep_id_part: bool = False,
        drop_column: list[str] | None = None,
    ) -> polars.DataFrame:
        """Add genotype information to variants DataFrame."""
        variants = sake.utils.add_id_part(variants)
        path_with_target = pathlib.Path(str(self.partitions_path).format(target=target))

        if drop_column is None:
            drop_column = []

        if keep_id_part:
            drop_column.append("id_part")

        all_genotypes = []
        for (id_part, *_), data in variants.group_by(["id_part"]):
            part_path = path_with_target / f"id_part={id_part}/0.parquet"

            if not part_path.is_file():
                continue

            query = """
            select
                v.*, g.sample, g.gt, g.ad, g.dp, g.gq,
            from
                $variants as v
            left join
                read_parquet('$path') as g
            on
                v.id == g.id
            """

            result = (
                self.__db.execute(
                    query,
                    {
                        "variants": data,
                        "path": part_path,
                    },
                )
                .pl()
                .with_columns(
                    ad=polars.col("ad").cast(polars.List(polars.String)).list.join(","),
                )
                .drop(drop_column)
            )

            all_genotypes.append(result)

        return polars.concat(all_genotypes)

__all__: list[str] = ["Sake"]
