"""Some utils function."""

from __future__ import annotations

# std import
import typing

# 3rd party import
import polars

# project import

__all__ = ["add_id_part", "add_recurrence", "list2string", "get_list"]


def add_id_part(data: polars.DataFrame) -> polars.DataFrame:
    """Compute and add id_part of polars.DataFrame."""
    # it's look like dark magic but it's just bit shift without bit shift operator
    return data.with_columns(
        id_part=polars.when(polars.col("id") // 9223372036854775808 == 1)
        .then(
            255,
        )
        .otherwise(
            polars.col("id") * 2 // 72057594037927936,
        ),
    )


def add_recurrence(data: polars.DataFrame) -> polars.DataFrame:
    """Compute recurrence of variant.

    Requirement:
    - id: variant id
    - gt: genotype
    """
    recurrence = data.group_by("id").agg(
        variantplaner_AC=polars.sum("gt"),
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
