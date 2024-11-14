"""Test obj submodule."""

from __future__ import annotations

# std import
import pathlib

# 3rd party import
import polars

# project import
from sake import Sake, utils


def test_id_part() -> None:
    """Check compute id_part."""
    ids = polars.DataFrame(
        {
            "id": [
                630599235976298501,
                2617780672831422469,
                5395614109402136580,
                6369726425292865540,
                65458578651086855,
                2015481302785982471,
                5981677259675664388,
                6211304379031486564,
                5098708148420608007,
                5767738414606581766,
                3434313503375097860,
                5652739209262268423,
                380787391679430674,
                5430200770219737092,
                2468924581742641156,
                727479041598160903,
                1467709823924707872,
                2849212168077836294,
                6566373719143350278,
                728255311839756293,
            ]
        },
        schema={
            "id": polars.UInt64,
        },
    )

    ids = utils.add_id_part(ids)

    assert ids.get_column("id_part").to_list() == [
        17,
        72,
        149,
        176,
        1,
        55,
        166,
        172,
        141,
        160,
        95,
        156,
        10,
        150,
        68,
        20,
        40,
        79,
        182,
        20,
    ]


def test_recurrence() -> None:
    """Check recurrence."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "10", 79257338, 121966721)
    genotyped = sake.add_genotypes(variants, "germline")
    recurrence = utils.add_recurrence(genotyped).select("id", "sake_AC").unique("id")

    print(recurrence.get_column("id").to_list())
    print(recurrence.get_column("sake_AC").to_list())
    truth = polars.DataFrame(
        {
            "id": [
               3779432198831079429, 3808437564679913478, 3854716627568295943, 3858706742380593160, 3802997657887047684, 3767474835932839940, 3798315761282318341, 3790995582231773212, 3817462055472988165
            ],
            "sake_AC": [1, 4, 6, 5, 3, 1, 6, 2, 5],
        },
        schema={
            "id": polars.UInt64,
            "sake_AC": polars.Int64,
        },
    )

    polars.testing.assert_frame_equal(recurrence, truth, check_row_order=False, check_column_order=False)
