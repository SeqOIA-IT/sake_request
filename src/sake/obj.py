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
    preindication: str = dataclasses.field(kw_only=False)

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
                    str_value = str_value.format(target=self.preindication)

                self.__setattr__(key, self.sake_path / str_value)

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
        fix_version = sake._utils.fix_annotation_version(name, version, self.preindication)

        # annotations_path are set in __post_init__
        annotation_path = self.annotations_path / f"{name}" / f"{fix_version}"  # type: ignore[operator]

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

        query = sake.QUERY["add_annotations"].format(columns=columns)

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

        query = sake.QUERY["add_sample_info"].format(columns=columns)

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

        query = sake.QUERY["add_transmissions"].format(columns=input_columns)

        for (pid_crc, *_), _data in iterator:
            path = self.transmissions_path / f"{pid_crc}.parquet"  # type: ignore[operator]

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

    def add_variants(self, _data: polars.DataFrame) -> polars.DataFrame:
        """Use id of column polars.DataFrame to get variant information."""
        return self.db.execute(
            sake.QUERY["add_variants"],
            {
                "path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
            },
        ).pl()

    def all_variants(self) -> polars.DataFrame:
        """Get all variants of a target in present in Sake."""
        return self.db.execute(
            sake.QUERY["all_variants"],
            {
                "path": sake._utils.fix_variants_path(self.variants_path, None),  # type: ignore[arg-type]
            },
        ).pl()

    def get_annotations(
        self,
        name: str,
        version: str,
        *,
        rename_column: bool = True,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame:
        """Get all variants of an annotations."""
        fix_version = sake._utils.fix_annotation_version(name, version, self.preindication)

        annotation_path = self.annotations_path / f"{name}" / f"{fix_version}"  # type: ignore[operator]

        schema = polars.read_parquet_schema(annotation_path / "1.parquet")
        chromosomes_list = [
            entry.name.split(".")[0]
            for entry in os.scandir(annotation_path)
            if entry.is_file() and entry.name.endswith(".parquet")
        ]

        if "id" in schema:
            del schema["id"]
            columns = ",".join([f"a.{col}" for col in schema if select_columns is None or col in select_columns])

        query = sake.QUERY["get_annotations"].format(columns=columns)

        all_annotations = []
        iterator = sake._utils.wrap_iterator(self.activate_tqdm, chromosomes_list)  # type: ignore[arg-type]
        for chrom in iterator:
            result = self.db.execute(
                query,
                {
                    "annotation_path": str(
                        annotation_path / f"{chrom}.parquet",
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

    def get_cnv(
        self,
        chrom: str,
        start: int,
        stop: int,
        tools: str,
        sv_type: str,
        *,
        exact: bool = True,
    ) -> polars.DataFrame:
        """Get cnv from chromosome between start and stop."""
        start_comp = "==" if exact else ">"
        stop_comp = "==" if exact else "<"

        return self.db.execute(
            sake.QUERY["get_cnv"].format(start_comp=start_comp, stop_comp=stop_comp),
            {
                "path": str(self.cnv_path / "groupby" / tools / sv_type / f"{chrom}.parquet"),  # type: ignore[operator]
                "start": start,
                "stop": stop,
            },
        ).pl()

    def get_cnv_by_sample(self, sample: str, tools: str) -> polars.DataFrame:
        """Get cnv by sample."""
        return polars.read_parquet(self.cnv_path / "samples" / sample / f"{tools}.parquet")  # type: ignore[operator]

    def get_interval(self, chrom: str, start: int, stop: int) -> polars.DataFrame:
        """Get variants from chromosome between start and stop."""
        return self.db.execute(
            sake.QUERY["get_interval"],
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
        return self.db.execute(
            sake.QUERY["get_variant_of_prescription"],
            {
                "sample_path": str(
                    self.prescriptions_path / f"{prescription}.parquet",  # type: ignore[operator]
                ),
                "variant_path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
            },
        ).pl()

    def get_variant_of_prescriptions(self, prescriptions: list[str]) -> polars.DataFrame:
        """Get all variants of multiple prescriptions."""
        iterator = sake._utils.wrap_iterator(self.activate_tqdm, prescriptions)  # type: ignore[arg-type]

        all_variants = []
        for pid in iterator:
            all_variants.append(
                self.db.execute(
                    sake.QUERY["get_variant_of_prescription"],
                    {
                        "sample_path": str(self.prescriptions_path / f"{pid}.parquet"),  # type: ignore[operator]
                        "variant_path": sake._utils.fix_variants_path(self.variants_path),  # type: ignore[arg-type]
                    },
                ).pl(),
            )

        return polars.concat(all_variants)
