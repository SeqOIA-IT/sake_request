"""Some utils function."""

from __future__ import annotations

# std import
import pathlib
import typing

# 3rd party import
import polars

# project import

if typing.TYPE_CHECKING:
    # 3rd party import
    import duckdb


__all__ = ["GenotypeQuery", "add_id_part", "add_recurrence", "fix_variants_path", "get_list", "list2string"]


def add_id_part(data: polars.DataFrame, number_of_bits: int = 8) -> polars.DataFrame:
    """Compute and add id_part of polars.DataFrame."""
    # it's look like dark magic but it's just bit shift without bit shift operator
    return data.with_columns(
        id_part=polars.when(polars.col("id") // pow(2, 63) == 1)
        .then(
            pow(2, number_of_bits) - 1,
        )
        .otherwise(
            polars.col("id") * 2 // pow(2, 64 - number_of_bits),
        ),
    )


def add_recurrence(data: polars.DataFrame) -> polars.DataFrame:
    """Compute recurrence of variant.

    Requirement:
    - id: variant id
    - gt: genotype
    """
    recurrence = data.group_by("id").agg(
        sake_AC=polars.sum("gt"),
    )

    return data.join(recurrence, on="id", how="left")


def list2string(data: polars.DataFrame, *, columns: list[str], separator: str = ",") -> polars.DataFrame:
    """Convert list in string."""
    return data.with_columns(
        [polars.col(name).cast(polars.List(polars.String)).list.join(separator).alias(name) for name in columns],
    )


def get_list(
    data: polars.DataFrame,
    *,
    columns: list[str],
    index: int = 0,
    null_value: typing.Any = 0,
) -> polars.DataFrame:
    """Replace list by value at index or null_value if index is out of bound."""
    return data.with_columns(
        [polars.col(name).list.get(index, null_on_oob=True).fill_null(null_value).alias(name) for name in columns],
    )


def fix_variants_path(path: pathlib.Path, target: str, chrom: str | None = None) -> str:
    """Fix variants path to match if variants are split or not."""
    if chrom is None and pathlib.Path(str(path).format(target=target)).is_dir():
        return str(path).format(target=target) + "/*.parquet"
    if pathlib.Path(str(path).format(target=target)).is_dir():
        return str(path).format(target=target) + f"/{chrom}.parquet"
    return str(path.with_suffix(".parquet")).format(target=target)


class GenotypeQuery:
    """Class to run genotype quering."""
    def __init__(self, db: duckdb.DuckDBPyConnection, prefix: pathlib.Path, drop_column: list[str]):
        """Create genotyping query object."""
        self.db = db
        self.prefix = prefix
        self.drop_column = drop_column

    def __call__(self, params: tuple[tuple[int, typing.Any], polars.DataFrame]) -> polars.DataFrame | None:
        """Run genotyping of data with information in path."""
        (id_part, *_), _data = params

        part_path = self.prefix / f"id_part={id_part}/0.parquet"
        if not part_path.is_file():
            return None

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

        return (
            self.db.execute(
                query,
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
