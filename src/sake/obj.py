"""Define Sake dataclass, main API of sake_request."""

from __future__ import annotations

import dataclasses

# std import
import multiprocessing
import os
import pathlib

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
    "genotype_columns": ["gt", "ad", "dp", "gq"],
}


@dataclasses.dataclass(kw_only=True)
class Sake:
    """Class that help user to extract variants from sake."""

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
    genotype_columns: list[str] | None = None

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

                if key.endswith("path"):
                    self.__setattr__(key, self.sake_path / str_value)
                else:
                    self.__setattr__(key, value)

    def add_annotations(
        self,
        variants: polars.DataFrame,
        name: str,
        version: str,
        *,
        rename_column: bool = True,
        select_columns: list[str] | None = None,
        read_threads: int = 1,
    ) -> polars.DataFrame:
        """Add annotations to variants.

        Require `id` column in variants value.

        Parameters:
          variants: DataFrame you wish to annotate
          name: Name of annotations you want add to your variants
          version: version of annotations you want add to your variants
          rename_column: prefix annotations column name with annotations name
          select_columns: name of annotations column (same as is in annotations file) you want add to your DataFrame, if None all column are added

        Return:
          DataFrame with annotations column.
        """
        annotation_path_result = sake._utils.fix_annotation_path(
            self.annotations_path,  # type: ignore[arg-type]
            name,
            version,
            self.preindication,
        )
        if annotation_path_result is not None:
            (annotation_path, split_by_chr) = annotation_path_result
        else:
            # No annotations path return input
            return variants

        schema = polars.read_parquet_schema(annotation_path)
        if "id" in schema:
            del schema["id"]
        columns = ",".join([f"a.{col}" for col in schema if select_columns is None or col in select_columns])

        if split_by_chr:
            iterator = sake._utils.wrap_iterator(
                self.activate_tqdm,  # type: ignore[arg-type]
                variants.group_by(["chr"]),
                total=variants.get_column("chr").unique().len(),
            )
            annotation_path = annotation_path.parent

            query_obj = sake._utils.QueryByGroupBy(
                self.threads // read_threads,  # type: ignore[operator]
                f"{annotation_path}/{{}}.parquet",
                "add_annotations",
                {"columns": columns},
                select_columns,
            )
            if read_threads == 1:
                all_annotations = list(map(query_obj, iterator))
            else:
                with multiprocessing.get_context("spawn").Pool(processes=read_threads) as pool:
                    all_annotations = list(pool.imap(query_obj, iterator))

            result = polars.concat([df for df in all_annotations if df is not None])
        else:
            query_str = sake.QUERY["add_annotations"].format(columns=columns)

            result = self.db.execute(
                query_str,
                {
                    "path": annotation_path,
                },
            ).pl()

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
        select_columns: list[str] | None = None,
        number_of_bits: int = 8,
        read_threads: int = 1,
    ) -> polars.DataFrame:
        """Add genotype information to variants DataFrame.

        Require `id` column in variants value.

        Parameters:
          variants: DataFrame you wish to add genotypes
          keep_id_part: method add id_part column, set to True to keep_it
          select_columns: name of genotype column you want add to your DataFrame, if None all column are added
          number_of_bits: number of bits use to compute partitions
          read_threads: number of partitions file read in parallel

        Return:
          DataFrame with genotype information.
        """
        if select_columns is None:
            select_columns = [
                *variants.schema.names(),
                "sample",
                *self.genotype_columns,  # type: ignore[misc]
            ]
        else:
            select_columns = [*variants.schema.names(), "sample", *select_columns]

        variants = sake.utils.add_id_part(variants, number_of_bits=number_of_bits)

        if keep_id_part:
            select_columns.append("id_part")

        all_genotypes: list[polars.DataFrame | None] = []
        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            variants.group_by(["id_part"]),
            total=variants.get_column("id_part").unique().len(),
        )

        query = sake._utils.QueryByGroupBy(
            self.threads // read_threads,  # type: ignore[operator]
            f"{self.partitions_path}/id_part={{}}/0.parquet",
            "genotype_query",
            select_columns=select_columns,
            expressions=[
                polars.col("ad").cast(polars.List(polars.String)).list.join(",").alias("ad"),
            ],
        )

        if read_threads == 1:
            all_genotypes = list(map(query, iterator))
        else:
            with multiprocessing.get_context("spawn").Pool(processes=read_threads) as pool:
                all_genotypes = list(pool.imap(query, iterator))

        return polars.concat([df for df in all_genotypes if df is not None])

    def add_sample_info(
        self,
        _variants: polars.DataFrame,
        *,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame:
        """Add sample information.

        Required sample column in polars.DataFrame.

        Parameters:
          _variants: DataFrame you wish to add sample information
          select_columns: name of sample information column you want add to your DataFrame, if None all column are added

        Return:
          DataFrame with sample information.
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
        *,
        select_columns: list[str] | None = None,
        read_threads: int = 1,
    ) -> polars.DataFrame:
        """Add transmissions information.

        Required pid_crc column in polars.DataFrame.


        Parameters:
          variants: DataFrame you wish to add genotypes
          select_columns: name of transmissions column you want add to your DataFrame, if None all column are added
          read_threads: number of partitions file read in parallel

        Return:
          DataFrame with genotype information.
        """
        if select_columns is None:
            select_columns = list(variants.schema)
            select_columns += [
                f"{prefix}_{suffix}"
                for suffix in self.genotype_columns  # type: ignore[union-attr]
                for prefix in ["index", "father", "mother"]
            ]
            select_columns += ["origin"]
        else:
            select_columns = [*variants.schema.names(), *select_columns, "origin"]

        all_transmissions = []
        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            variants.group_by(["pid_crc"]),
            total=variants.get_column("pid_crc").unique().len(),
        )
        query = sake._utils.QueryByGroupBy(
            self.threads // read_threads,  # type: ignore[operator]
            f"{self.transmissions_path}/{{}}.parquet",
            "add_transmissions",
            select_columns=select_columns,
            expressions=[
                polars.col("father_gt").cast(polars.UInt8).alias("father_gt"),
                polars.col("index_gt").cast(polars.UInt8).alias("index_gt"),
                polars.col("mother_gt").cast(polars.UInt8).alias("mother_gt"),
                polars.col("father_dp").cast(polars.UInt32).alias("father_dp"),
                polars.col("index_dp").cast(polars.UInt32).alias("index_dp"),
                polars.col("mother_dp").cast(polars.UInt32).alias("mother_dp"),
                polars.col("father_gq").cast(polars.UInt32).alias("father_gq"),
                polars.col("index_gq").cast(polars.UInt32).alias("index_gq"),
                polars.col("mother_gq").cast(polars.UInt32).alias("mother_gq"),
                polars.col("father_ad").cast(polars.List(polars.String)).list.join(",").alias("father_ad"),
                polars.col("index_ad").cast(polars.List(polars.String)).list.join(",").alias("index_ad"),
                polars.col("mother_ad").cast(polars.List(polars.String)).list.join(",").alias("mother_ad"),
            ],
        )

        if read_threads == 1:
            all_transmissions = list(map(query, iterator))
        else:
            with multiprocessing.get_context("spawn").Pool(processes=read_threads) as pool:
                all_transmissions = list(pool.imap(query, iterator))

        return polars.concat([df for df in all_transmissions if df is not None])

    def __add_all_variants(self, name: str, _data: polars.DataFrame | None = None) -> polars.DataFrame:
        """Merge add and all variants code."""
        iterator = sake._utils.wrap_iterator(
            self.activate_tqdm,  # type: ignore[arg-type]
            sake._utils.get_chromosome_path(self.variants_path),  # type: ignore[arg-type]
        )

        all_variants = []
        for path in iterator:
            all_variants.append(
                self.db.execute(
                    sake.QUERY[name],
                    {
                        "path": str(path),
                    },
                ).pl(),
            )

        return polars.concat(all_variants)

    def add_variants(self, _data: polars.DataFrame) -> polars.DataFrame:
        """Use id of column polars.DataFrame to get variant information."""
        return self.__add_all_variants("add_variants", _data)

    def all_variants(self) -> polars.DataFrame:
        """Get all variants of a target in present in Sake."""
        return self.__add_all_variants("all_variants")

    def get_annotations(
        self,
        name: str,
        version: str,
        *,
        rename_column: bool = True,
        select_columns: list[str] | None = None,
    ) -> polars.DataFrame | None:
        """Get all variants of an annotations.

        Parameters:
          name: Name of annotations you want add to your variants
          version: version of annotations you want add to your variants
          rename_column: prefix annotations column name with annotations name
          select_columns: name of annotations column (same as is in annotations file) you want add to your DataFrame, if None all column are added

        Return:
          DataFrame with annotations column.
        """
        annotation_path_result = sake._utils.fix_annotation_path(
            self.annotations_path,  # type: ignore[arg-type]
            name,
            version,
            self.preindication,
        )
        if annotation_path_result is not None:
            (annotation_path, split_by_chr) = annotation_path_result
        else:
            # No annotations path return input
            return None

        schema = polars.read_parquet_schema(annotation_path)
        if "id" in schema:
            del schema["id"]
        columns = ",".join([f"a.{col}" for col in schema if select_columns is None or col in select_columns])

        query = sake.QUERY["get_annotations"].format(columns=columns)
        if split_by_chr:
            annotations_path = sake._utils.get_chromosome_path(annotation_path.parent)
            variants_path = sake._utils.get_chromosome_path(self.variants_path)  # type: ignore[arg-type]
            iterator = sake._utils.wrap_iterator(
                self.activate_tqdm,  # type: ignore[arg-type]
                zip(annotations_path, variants_path),
            )

            all_annotations = []
            for annotation_path, variant_path in iterator:
                chrom_result = self.db.execute(
                    query,
                    {
                        "annotation_path": str(annotation_path),
                        "variant_path": str(variant_path),
                    },
                ).pl()

                all_annotations.append(chrom_result)

            result = polars.concat([df for df in all_annotations if df is not None])
        else:
            result = self.db.execute(
                query,
                {
                    "annotation_path": annotation_path,
                    "variant_path": f"{self.variants_path}/*.parquet",
                },
            ).pl()

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
                "path": str(self.variants_path / f"{chrom}.parquet"),  # type: ignore[operator]
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
                "variant_path": f"{self.variants_path}/*.parquet",
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
                        "variant_path": f"{self.variants_path}/*.parquet",
                    },
                ).pl(),
            )

        return polars.concat(all_variants)
