"""Some utils function."""

from __future__ import annotations

# std import
# 3rd party import
import polars

# project import


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
        sake_AC=polars.sum("gt"),
    )

    return data.join(recurrence, on="id", how="left")
