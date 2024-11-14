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
    "partitions_path": pathlib.Path("{target}") / "genotypes" / "partitions",
    "samples_path": pathlib.Path("samples") / "patients.parquet",
    "transmissions_path": pathlib.Path("{target}") / "genotypes" / "transmissions",
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
    partitions_path: pathlib.Path | None = None
    samples_path: pathlib.Path | None = None
    transmissions_path: pathlib.Path | None = None
    variants_path: pathlib.Path | None = None

    # Private member
    __db: duckdb.DuckDBPyConnection = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self.__db = duckdb.connect(":memory:", config={"threads": self.threads})
        os.environ["POLARS_MAX_THREADS"] = str(self.threads)

        for default_field in DEFAULT_PATH:
            if self.__getattribute__(default_field) is None:
                self.__setattr__(default_field, self.sake_path / DEFAULT_PATH[default_field])

    def get_interval(self, target: str, chrom: str, start: int, stop: int) -> polars.DataFrame:
        """Get variants from chromosome between start and stop."""
        query = """
        select
            v.id, v.chr, v.pos, v.ref, v.alt
        from
            read_parquet($path) as v
        where
            v.chr == $chrom
        and
            v.pos > $start
        and
            v.pos > $stop
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

    def add_variants(self, _data: polars.DataFrame, target: str) -> polars.DataFrame:
        """Use id of column polars.DataFrame to get variant information."""
        query = """
        select
            v.chr, v.pos, v.ref, v.alt, d.*
        from
            read_parquet($path) as v
        join
            _data as d
        on
            v.id == d.id
        """

        return self.__db.execute(
            query,
            {
                "path": str(self.variants_path).format(target=target),
            },
        ).pl()

    def add_genotypes(
        self,
        variants: polars.DataFrame,
        target: str,
        *,
        keep_id_part: bool = False,
        drop_column: list[str] | None = None,
    ) -> polars.DataFrame:
        """Add genotype information to variants DataFrame.

        Require `id` column in variants value
        """
        variants = sake.utils.add_id_part(variants)
        path_with_target = pathlib.Path(str(self.partitions_path).format(target=target))

        if drop_column is None:
            drop_column = []

        if not keep_id_part:
            drop_column.append("id_part")

        all_genotypes = []
        for (id_part, *_), _data in variants.group_by(["id_part"]):
            part_path = path_with_target / f"id_part={id_part}/0.parquet"

            if not part_path.is_file():
                continue

            query = """
            select
                v.*, g.sample, g.gt, g.ad, g.dp, g.gq
            from
                _data as v
            left join
                read_parquet($path) as g
            on
                v.id == g.id
            """

            result = (
                self.__db.execute(
                    query,
                    {
                        "path": str(part_path),
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

    def add_annotations(
        self,
        variants: polars.DataFrame,
        name: str,
        version: str,
        *,
        rename_column: bool = True,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame:
        """Add annotations to variants.

        Require `id` column in variants value
        """
        # annotations_path are set in __post_init__
        annotation_path = self.annotations_path / f"{name}" / f"{version}"  # type: ignore[operator]

        schema = polars.read_parquet_schema(annotation_path / "1.parquet")
        if "id" in schema:
            del schema["id"]
        columns = ",".join([f"a.{col}" for col in schema if select_columns is None or col in select_columns])

        all_annotations = []
        for (chrom, *_), _data in variants.group_by(["chr"]):
            query = f"""
            select
                v.*, {columns}
            from
                _data as v
            left join
                read_parquet($path) as a
            on
                v.id == a.id
            """  # noqa: S608 we accept risk of sql inject

            result = self.__db.execute(
                query,
                {
                    "path": str(annotation_path / f"{chrom}.parquet"),
                },
            ).pl()

            all_annotations.append(result)

        result = polars.concat(all_annotations)

        if rename_column:
            result = result.rename(
                {col: f"{name}_{col}" for col in schema if select_columns is None or col in select_columns},
            )

        return result

    def add_sample_info(
        self,
        _variants: polars.DataFrame,
        *,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame:
        """Add sample information.

        Required sample column in polars.DataFrame.
        """
        # sampless_path are set in __post_init__
        schema = polars.read_parquet_schema(self.samples_path)  # type: ignore[arg-type]
        columns = ",".join([f"s.{col}" for col in schema if select_columns is None or col in select_columns])

        query = f"""
        select
            v.*, {columns}
        from
            _variants as v
        left join
            read_parquet($path) as s
        on
            v.sample == s.sample
        """  # noqa: S608 we accept risk of sql inject

        return self.__db.execute(
            query,
            {
                "path": str(self.samples_path),
            },
        ).pl()

    def add_transmissions(
        self,
        variants: polars.DataFrame,
    ) -> polars.DataFrame:
        """Add transmissions information.

        Required pid_crc column in polars.DataFrame.
        """
        all_transmissions = []
        for (pid_crc, *_), _data in variants.group_by(["pid_crc"]):
            path = pathlib.Path(str(self.transmissions_path).format(target="germline")) / f"{pid_crc}.parquet"

            if not path.is_file():
                continue

            query = """
            select
                v.*, t.*
            from
                _data as v
            left join
                read_parquet($path) as t
            on
                v.id == t.id
            """

            result = (
                self.__db.execute(
                    query,
                    {
                        "path": str(path),
                    },
                )
                .pl()
                .cast(
                    {
                        "father_gt": polars.UInt8,
                        "mother_gt": polars.UInt8,
                        "index_gt": polars.UInt8,
                        "father_dp": polars.UInt32,
                        "mother_dp": polars.UInt32,
                        "father_gq": polars.UInt32,
                        "mother_gq": polars.UInt32,
                    },
                )
                .with_columns(
                    father_ad=polars.col("father_ad").cast(polars.List(polars.String)).list.join(","),
                    mother_ad=polars.col("mother_ad").cast(polars.List(polars.String)).list.join(","),
                    index_ad=polars.col("index_ad").cast(polars.List(polars.String)).list.join(","),
                )
            )

            all_transmissions.append(result)

        return polars.concat(all_transmissions)


__all__: list[str] = ["Sake"]
