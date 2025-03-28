"""Define Sake dataclass."""

from __future__ import annotations

# std import
import multiprocessing
import os
import pathlib
import sys

if sys.version_info[:2] <= (3, 9):
    from sake import dataclasses
else:
    import dataclasses

# 3rd party import
import duckdb
import polars

# project import
import sake

__all__: list[str] = ["Sake"]


DEFAULT_PATH = {
    "aggregations_path": "aggregations",
    "annotations_path": "annotations",
    "cnv_path": pathlib.Path("{target}") / "cnv",
    "partitions_path": pathlib.Path("{target}") / "genotypes" / "partitions",
    "prescriptions_path": pathlib.Path("{target}") / "genotypes" / "samples",
    "samples_path": pathlib.Path("samples") / "patients.parquet",
    "str_path": pathlib.Path("{target}") / "str",
    "transmissions_path": pathlib.Path("{target}") / "genotypes" / "transmissions",
    "variants_path": pathlib.Path("{target}") / "variants",
}


@dataclasses.dataclass(kw_only=True)
class Sake:
    """Class that let user extract variants from sake."""

    # Mandatory member
    sake_path: pathlib.Path = dataclasses.field(kw_only=False)
    target: str = dataclasses.field(kw_only=False)

    # Optional member
    threads: int | None = dataclasses.field(default=os.cpu_count())
    activate_tqdm: bool | None = dataclasses.field(default=False)

    # Optional member generate from sake_path
    aggregations_path: pathlib.Path | None = None
    annotations_path: pathlib.Path | None = None
    cnv_path: pathlib.Path | None = None
    partitions_path: pathlib.Path | None = None
    prescriptions_path: pathlib.Path | None = None
    samples_path: pathlib.Path | None = None
    str_path: pathlib.Path | None = None
    transmissions_path: pathlib.Path | None = None
    variants_path: pathlib.Path | None = None

    # duckdb connection
    db: duckdb.DuckDBPyConnection = dataclasses.field(init=False, repr=False)

    def __post_init__(self):
        self.db = duckdb.connect(
            ":memory:",
        )
        self.db.query("SET enable_progress_bar = false;")
        self.db.query(f"SET threads TO {self.threads};")
        os.environ["POLARS_MAX_THREADS"] = str(self.threads)

        for key, value in DEFAULT_PATH.items():
            if self.__getattribute__(key) is None:
                str_value = str(value)
                if "{target}" in str_value:
                    str_value = str_value.format(target=self.target)

                self.__setattr__(key, self.sake_path / str_value)

    def all_variants(self) -> polars.DataFrame:
        """Get all variants of a target in present in Sake."""
        query = """
        select
            v.id, v.chr, v.pos, v.ref, v.alt
        from
            read_parquet($path) as v
        """

        return self.db.execute(
            query,
            {
                "path": sake._utils.fix_variants_path(self.variants_path, None),  # type: ignore[arg-type]
            },
        ).pl()

    def get_interval(self, chrom: str, start: int, stop: int) -> polars.DataFrame:
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
            v.pos < $stop
        """

        return self.db.execute(
            query,
            {
                "path": sake._utils.fix_variants_path(self.variants_path, chrom),  # type: ignore[arg-type]
                "chrom": chrom,
                "start": start,
                "stop": stop,
            },
        ).pl()

    def get_intervals(self, chroms: list[str], starts: list[int], stops: list[int]) -> polars.DataFrame:
        """Get variants in multiple intervals."""
        all_variants = []
        minimal_length = min(len(chroms), len(starts), len(stops))
        iterator = sake._utils.wrap_iterator(self.activate_tqdm, zip(chroms, zip(starts, stops)), total=minimal_length)  # type: ignore[arg-type]

        for chrom, (start, stop) in iterator:
            all_variants.append(
                self.get_interval(chrom, start, stop),
            )

        return polars.concat(all_variants)

    def get_variant_of_prescription(self, prescription: str) -> polars.DataFrame:
        """Get all variants of a prescription."""
        query = """
        select
            v.chr, v.pos, v.ref, v.alt, g.*
        from
            read_parquet($sample_path) as g
        join
            read_parquet($variant_path) as v
        on
            v.id = g.id
        """

        return self.db.execute(
            query,
            {
                "sample_path": str(
                    self.prescriptions_path / f"{prescription}.parquet",  # type: ignore[operator]
                ),
                "variant_path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
            },
        ).pl()

    def get_variant_of_prescriptions(self, prescriptions: list[str]) -> polars.DataFrame:
        """Get all variants of multiple prescriptions."""
        query = """
        select
            v.chr, v.pos, v.ref, v.alt, g.*
        from
            read_parquet($sample_path) as g
        join
            read_parquet($variant_path) as v
        on
            v.id = g.id
        """

        iterator = sake._utils.wrap_iterator(self.activate_tqdm, prescriptions)  # type: ignore[arg-type]

        all_variants = []
        for pid in iterator:
            all_variants.append(
                self.db.execute(
                    query,
                    {
                        "sample_path": str(self.prescriptions_path / f"{pid}.parquet"),  # type: ignore[operator]
                        "variant_path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
                    },
                ).pl(),
            )

        return polars.concat(all_variants)

    def get_annotations(
        self,
        name: str,
        version: str,
        *,
        rename_column: bool = True,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame:
        """Get all variants of an annotations."""
        annotation_path = self.annotations_path / f"{name}" / f"{version}"  # type: ignore[operator]

        schema = polars.read_parquet_schema(annotation_path / "1.parquet")
        chromosomes_list = [
            entry.name.split(".")[0]
            for entry in os.scandir(annotation_path)
            if entry.is_file() and entry.name.endswith(".parquet")
        ]

        if "id" in schema:
            del schema["id"]
            columns = ",".join([f"a.{col}" for col in schema if select_columns is None or col in select_columns])

        query = f"""
        select
            v.*, {columns}
        from
            read_parquet($annotation_path) as a
        join
            read_parquet($variant_path) as v
        on
            v.id = a.id
        """  # noqa: S608 we accept risk of sql inject

        all_annotations = []
        iterator = sake._utils.wrap_iterator(self.activate_tqdm, chromosomes_list)  # type: ignore[arg-type]
        for chrom in iterator:
            result = self.db.execute(
                query,
                {
                    "annotation_path": str(
                        self.annotations_path / f"{name}" / f"{version}" / f"{chrom}.parquet",  # type: ignore[operator]
                    ),
                    "variant_path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
                },
            ).pl()

            all_annotations.append(result)

        result = polars.concat(all_annotations)

        if rename_column:
            result = result.rename(
                {col: f"{name}_{col}" for col in schema if select_columns is None or col in select_columns},
            )

        return result

    def add_variants(self, _data: polars.DataFrame) -> polars.DataFrame:
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

        return self.db.execute(
            query,
            {
                "path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
            },
        ).pl()

    def add_genotypes(
        self,
        variants: polars.DataFrame,
        *,
        keep_id_part: bool = False,
        drop_column: list[str] | None = None,
        number_of_bits: int = 8,
        read_threads: int = 1,
    ) -> polars.DataFrame:
        """Add genotype information to variants DataFrame.

        Require `id` column in variants value
        """
        variants = sake.utils.add_id_part(variants, number_of_bits=number_of_bits)

        if drop_column is None:
            drop_column = []

        if not keep_id_part:
            drop_column.append("id_part")

        all_genotypes: list[polars.DataFrame | None] = []
        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            variants.group_by(["id_part"]),
            total=variants.get_column("id_part").unique().len(),
        )

        if read_threads == 1:
            query = sake._utils.GenotypeQuery(self.threads, self.partitions_path, drop_column)  # type: ignore[arg-type]
            all_genotypes = list(map(query, iterator))
        else:
            duckdb_threads = self.threads // read_threads  # type: ignore[operator]
            query = sake._utils.GenotypeQuery(duckdb_threads, self.partitions_path, drop_column)  # type: ignore[arg-type]
            self.db.query(f"SET threads TO {duckdb_threads};")

            with multiprocessing.get_context("spawn").Pool(processes=read_threads) as pool:
                all_genotypes = list(pool.imap(query, iterator))

            self.db.query(f"SET threads TO {self.threads}")

        return polars.concat([df for df in all_genotypes if df is not None])

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
        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            variants.group_by(["chr"]),
            total=variants.get_column("chr").unique().len(),
        )

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

        for (chrom, *_), _data in iterator:
            if not (annotation_path / f"{chrom}.parquet").is_file():
                continue

            result = self.db.execute(
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

        if select_columns is None:
            select_columns = [col for col in schema if col != "sample"]

        columns = ",".join([f"s.{col}" for col in schema if col in select_columns])

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

        return self.db.execute(
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

        input_columns = ",".join([f"v.{col}" for col in variants.schema if col != "id"])

        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            variants.group_by(["pid_crc"]),
            total=variants.get_column("pid_crc").unique().len(),
        )

        query = f"""
        select
            {input_columns}, t.*
        from
            _data as v
        left join
            read_parquet($path) as t
        on
            v.id == t.id
        where
            v.kindex == True
        """  # noqa: S608 we accept risk of sql inject

        for (pid_crc, *_), _data in iterator:
            path = pathlib.Path(str(self.transmissions_path).format(target="germline")) / f"{pid_crc}.parquet"

            if not path.is_file():
                continue

            result = (
                self.db.execute(
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
