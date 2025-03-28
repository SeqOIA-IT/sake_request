"""Test obj submodule."""

from __future__ import annotations

# std import
import os
import pathlib

# 3rd party import
import polars
import polars.testing

# project import
from sake import Sake

TRUTH = polars.DataFrame(
    {
        "id": [
            6369631684756766724,
            6275197860862492677,
            6316975329873231879,
            6308398058992304134,
            6368810012415885317,
            6369726425292865540,
            6337120946989563910,
            6386640816105848939,
            6369631684756766724,
            6275197860862492677,  # 9
            6316975329873231879,  # 8
            6368810012415885317,  # 7
            6369726425292865540,
            6337120946989563910,
            6386640816105848939,
            6369631684756766724,  # 3
            6369726425292865540,
            6337120946989563910,
        ],
        "sample": [
            "DDD2",
            "DDD1",
            "DDD1",
            "EEE1",
            "FFF1",
            "AAA2",
            "BBB2",
            "DDD2",
            "DDD1",
            "DDD0",
            "DDD0",
            "FFF0",
            "AAA1",
            "BBB1",
            "DDD0",
            "DDD0",
            "AAA0",
            "BBB0",
        ],
        "chr": ["X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X", "X"],
        "pos": [
            91089368,
            47115192,
            66569342,
            62575239,
            90706747,
            91133485,
            75950376,
            99009862,
            91089368,
            47115192,
            66569342,
            90706747,
            91133485,
            75950376,
            99009862,
            91089368,
            91133485,
            75950376,
        ],
        "ref": ["T", "T", "A", "A", "T", "G", "G", "T", "T", "T", "A", "T", "G", "G", "T", "T", "G", "G"],
        "alt": ["A", "C", "G", "T", "C", "A", "T", "TTG", "A", "C", "G", "C", "A", "T", "TTG", "A", "A", "T"],
        "id_part": [176, 174, 175, 175, 176, 176, 175, 177, 176, 174, 175, 176, 176, 175, 177, 176, 176, 175],
        "gt": [2, 1, 1, 1, 1, 2, 2, 1, 1, 1, 1, 1, 2, 2, 1, 2, 2, 2],
        "ad": [
            "0,15",
            "21,20",
            "20,23",
            "43,5",
            "22,15",
            "0,20",
            "0,49",
            "18,11",
            "11,20",
            "26,26",
            "25,24",
            "16,17",
            "0,43",
            "0,15",
            "1,1",
            "0,28",
            "0,44",
            "1,27",
        ],
        "dp": [15, 41, 43, 49, 37, 20, 49, 29, 31, 53, 49, 33, 43, 15, 2, 28, 44, 28],
        "gq": [45, 99, 99, 81, 99, 60, 99, 99, 99, 99, 99, 99, 99, 45, 2, 84, 99, 74],
        "snpeff_effect": [
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
        ],
        "snpeff_impact": [
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
            "MODIFIER",
        ],
        "snpeff_gene": [
            "RNU6-555P-AL121823.1",
            "RGN-RNU12-2P",
            "PKMP2-EDA2R",
            "KRT8P17-BX322784.1",
            "RNU6-555P-AL121823.1",
            "RNU6-555P-AL121823.1",
            "AL451105.2-AC233982.1",
            "AL157778.1-AL137843.1",
            "RNU6-555P-AL121823.1",
            "RGN-RNU12-2P",
            "PKMP2-EDA2R",
            "RNU6-555P-AL121823.1",
            "RNU6-555P-AL121823.1",
            "AL451105.2-AC233982.1",
            "AL157778.1-AL137843.1",
            "RNU6-555P-AL121823.1",
            "RNU6-555P-AL121823.1",
            "AL451105.2-AC233982.1",
        ],
        "snpeff_geneid": [
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000130988-ENSG00000201659",
            "ENSG00000235892-ENSG00000131080",
            "ENSG00000235124-ENSG00000236852",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
            "ENSG00000281566-ENSG00000270308",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000130988-ENSG00000201659",
            "ENSG00000235892-ENSG00000131080",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
            "ENSG00000281566-ENSG00000270308",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
        ],
        "snpeff_feature": [
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
        ],
        "snpeff_feature_id": [
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000130988-ENSG00000201659",
            "ENSG00000235892-ENSG00000131080",
            "ENSG00000235124-ENSG00000236852",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
            "ENSG00000281566-ENSG00000270308",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000130988-ENSG00000201659",
            "ENSG00000235892-ENSG00000131080",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
            "ENSG00000281566-ENSG00000270308",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000207515-ENSG00000206062",
            "ENSG00000226854-ENSG00000232332",
        ],
        "snpeff_bio_type": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_rank": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_hgvs_c": [
            "n.91089368T>A",
            "n.47115192T>C",
            "n.66569342A>G",
            "n.62575239A>T",
            "n.90706747T>C",
            "n.91133485G>A",
            "n.75950376G>T",
            "n.99009862_99009863insTG",
            "n.91089368T>A",
            "n.47115192T>C",
            "n.66569342A>G",
            "n.90706747T>C",
            "n.91133485G>A",
            "n.75950376G>T",
            "n.99009862_99009863insTG",
            "n.91089368T>A",
            "n.91133485G>A",
            "n.75950376G>T",
        ],
        "snpeff_hgvs_p": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_cdna_pos": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_cdna_len": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_cds_pos": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_cvs_len": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_aa_pos": ["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "kindex": [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            False,
            True,
            True,
            True,
            False,
            False,
            True,
            True,
            True,
            True,
        ],
        "gender": ["M", "F", "F", "F", "F", "M", "M", "M", "F", "F", "F", "F", "F", "F", "F", "F", "F", "F"],
        "link": [
            "pere",
            "mere",
            "mere",
            "mere",
            "mere",
            "pere",
            "pere",
            "pere",
            "mere",
            "patient",
            "patient",
            "patient",
            "mere",
            "mere",
            "patient",
            "patient",
            "patient",
            "patient",
        ],
        "affected": [
            False,
            True,
            True,
            False,
            False,
            False,
            False,
            False,
            True,
            True,
            True,
            True,
            False,
            False,
            True,
            True,
            True,
            True,
        ],
        "pid_crc": [
            "DDDD",
            "DDDD",
            "DDDD",
            "EEEE",
            "FFFF",
            "AAAA",
            "BBBB",
            "DDDD",
            "DDDD",
            "DDDD",
            "DDDD",
            "FFFF",
            "AAAA",
            "BBBB",
            "DDDD",
            "DDDD",
            "AAAA",
            "BBBB",
        ],
        "preindication": [
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
            "p1",
        ],
        "index_gt": [1, 1, 2, 2, 2, None, 2, 2, 2, 1, 1, 1, 1, 2, 1, 2, 2, 2],
        "index_ad": [
            "16,17",
            "16,17",
            "0,44",
            "0,44",
            "0,44",
            None,
            "1,27",
            "1,27",
            "1,27",
            "26,26",
            "25,24",
            "16,17",
            "26,26",
            "0,28",
            "1,1",
            "0,28",
            "0,44",
            "1,27",
        ],
        "index_dp": [33, 33, 44, 44, 44, None, 28, 28, 28, 53, 49, 33, 53, 28, 2, 28, 44, 28],
        "index_gq": [99, 99, 99, 99, 99, None, 74, 74, 74, 99, 99, 99, 99, 84, 2, 84, 99, 74],
        "mother_gt": [1, 1, 2, 2, 2, None, 2, 2, 2, 1, 1, 1, 1, 1, 0, 1, 2, 2],
        "mother_ad": [
            "22,15",
            "22,15",
            "0,43",
            "0,43",
            "0,43",
            None,
            "0,15",
            "0,15",
            "0,15",
            "21,20",
            "20,23",
            "22,15",
            "21,20",
            "11,20",
            None,
            "11,20",
            "0,43",
            "0,15",
        ],
        "mother_dp": [37, 37, 43, 43, 43, None, 15, 15, 15, 41, 43, 37, 41, 31, None, 31, 43, 15],
        "mother_gq": [99, 99, 99, 99, 99, None, 45, 45, 45, 99, 99, 99, 99, 99, None, 99, 99, 45],
        "father_gt": [0, 0, 2, 2, 2, None, 2, 2, 2, 0, 0, 0, 0, 2, 0, 2, 2, 2],
        "father_ad": [
            None,
            None,
            "0,20",
            "0,20",
            "0,20",
            None,
            "0,49",
            "0,49",
            "0,49",
            None,
            None,
            None,
            None,
            "0,15",
            None,
            "0,15",
            "0,20",
            "0,49",
        ],
        "father_dp": [None, None, 20, 20, 20, None, 49, 49, 49, None, None, None, None, 15, None, 15, 20, 49],
        "father_gq": [None, None, 60, 60, 60, None, 99, 99, 99, None, None, None, None, 45, None, 45, 60, 99],
        "origin": [
            '""!',
            '""!',
            "###",
            "###",
            "###",
            None,
            "###",
            "###",
            "###",
            '""!',
            '""!',
            '""!',
            '""!',
            '#"#',
            '"!!',
            '#"#',
            "###",
            "###",
        ],
    },
    schema={
        "id": polars.UInt64,
        "chr": polars.String,
        "pos": polars.UInt64,
        "ref": polars.String,
        "alt": polars.String,
        "id_part": polars.UInt64,
        "sample": polars.String,
        "gt": polars.UInt8,
        "ad": polars.String,
        "dp": polars.UInt32,
        "gq": polars.UInt32,
        "kindex": polars.Boolean,
        "gender": polars.String,
        "link": polars.String,
        "affected": polars.Boolean,
        "pid_crc": polars.String,
        "preindication": polars.String,
        "father_gt": polars.UInt8,
        "mother_gt": polars.UInt8,
        "index_gt": polars.UInt8,
        "father_dp": polars.UInt32,
        "mother_dp": polars.UInt32,
        "index_dp": polars.UInt32,
        "father_gq": polars.UInt32,
        "mother_gq": polars.UInt32,
        "index_gq": polars.UInt32,
        "father_ad": polars.String,
        "mother_ad": polars.String,
        "index_ad": polars.String,
        "origin": polars.String,
        "snpeff_effect": polars.String,
        "snpeff_impact": polars.String,
        "snpeff_gene": polars.String,
        "snpeff_geneid": polars.String,
        "snpeff_feature": polars.String,
        "snpeff_feature_id": polars.String,
        "snpeff_bio_type": polars.String,
        "snpeff_rank": polars.String,
        "snpeff_hgvs_c": polars.String,
        "snpeff_hgvs_p": polars.String,
        "snpeff_cdna_pos": polars.String,
        "snpeff_cdna_len": polars.String,
        "snpeff_cds_pos": polars.String,
        "snpeff_cvs_len": polars.String,
        "snpeff_aa_pos": polars.String,
    },
)


def test_default_value() -> None:
    """Check default object."""
    sake_path = pathlib.Path("sake")

    database = Sake(sake_path, "germline")

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(os.cpu_count())
    assert not database.activate_tqdm

    assert database.aggregations_path == sake_path / "aggregations"
    assert database.annotations_path == sake_path / "annotations"
    assert database.cnv_path == sake_path / "germline" / "cnv"
    assert database.partitions_path == sake_path / "germline" / "genotypes" / "partitions"
    assert database.prescriptions_path == sake_path / "germline" / "genotypes" / "samples"
    assert database.samples_path == sake_path / "samples" / "patients.parquet"
    assert database.str_path == sake_path / "germline" / "str"
    assert database.transmissions_path == sake_path / "germline" / "genotypes" / "transmissions"
    assert database.variants_path == sake_path / "germline" / "variants"


def test_set_value() -> None:
    """Check default object."""
    sake_path = pathlib.Path("sake")

    database = Sake(
        sake_path,
        "germline",
        activate_tqdm=True,
        threads=23,
        aggregations_path=sake_path / "other",
        annotations_path=sake_path / "other",
        partitions_path=sake_path / "other",
        prescriptions_path=sake_path / "other",
        samples_path=sake_path / "other",
        transmissions_path=sake_path / "other",
        variants_path=sake_path / "other",
    )

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(23)
    assert database.activate_tqdm

    assert database.aggregations_path == sake_path / "other"
    assert database.annotations_path == sake_path / "other"
    assert database.partitions_path == sake_path / "other"
    assert database.prescriptions_path == sake_path / "other"
    assert database.samples_path == sake_path / "other"
    assert database.transmissions_path == sake_path / "other"
    assert database.variants_path == sake_path / "other"


def test_get_all() -> None:
    """Check all variants."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.all_variants()

    truth = polars.read_parquet("tests/data/germline/variants.parquet")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_get_interval() -> None:
    """Check get interval."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_interval("X", 47115191, 99009863)

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt").unique("id")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_get_intervals() -> None:
    """Check get interval."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_intervals(["X", "10"], [47115191, 47115191], [99009863, 99009863])

    assert result.get_column("id").to_list() == [
        6369631684756766724,
        6275197860862492677,
        6316975329873231879,
        6308398058992304134,
        6368810012415885317,
        6369726425292865540,
        6337120946989563910,
        6386640816105848939,
        3766989042919407621,
        3802997657887047684,
        3759564262087852038,
        3721020433152081934,
        3767474835932839940,
        3798315761282318341,
        3808437564679913478,
        3779432198831079429,
        3790995582231773212,
        3757853205099184134,
        3743061537447739397,
        3729798117944460526,
    ]


def test_get_cnv() -> None:
    """Check get cnv."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_cnv("X", 220002, 156035000, "wisecondor", "DEL")
    assert result.get_column("chr").to_list() == ["X"]
    assert result.get_column("start").to_list() == [220002]
    assert result.get_column("end").to_list() == [156035000]
    assert result.get_column("tool").to_list() == ["wisecondor"]
    assert result.get_column("type").to_list() == ["DEL"]

    result = sake.get_cnv("X", 220002, 156035000, "wisecondor", "DEL", exact=False)
    assert result.get_column("chr").to_list() == ["X"] * 16
    assert result.get_column("start").sort().to_list() == [
        250002,
        1820002,
        1820002,
        25175002,
        32220002,
        32220002,
        33180002,
        48010002,
        48015002,
        49570002,
        56985002,
        58620002,
        73600002,
        73600002,
        82140002,
        117360002,
    ]
    assert result.get_column("end").sort().to_list() == [
        1910000,
        1910000,
        2780000,
        26825000,
        32440000,
        32440000,
        39730000,
        48120000,
        48130000,
        49600000,
        57090000,
        62405000,
        73610000,
        73610000,
        82160000,
        117445000,
    ]
    assert result.get_column("tool").to_list() == ["wisecondor"] * 16
    assert result.get_column("type").to_list() == ["DEL"] * 16


def test_get_cnv_by_sample() -> None:
    """Check get cnv by sample."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_cnv_by_sample("31E5EE", "wisecondor")
    assert result.get_column("chr").sort().to_list() == [
        "1",
        "10",
        "12",
        "15",
        "15",
        "15",
        "15",
        "15",
        "15",
        "18",
        "18",
        "18",
        "18",
        "18",
        "2",
        "2",
        "4",
        "6",
        "6",
        "7",
        "9",
        "9",
    ]
    assert result.get_column("start").sort().to_list() == [
        1905002,
        1905002,
        4570002,
        4570002,
        7845002,
        13250002,
        24105002,
        24140002,
        24140002,
        24195002,
        24325002,
        24425002,
        28590002,
        28590002,
        44710002,
        50480002,
        81370002,
        81370002,
        158600002,
        161030002,
        167930002,
        167930002,
    ]
    assert result.get_column("end").sort().to_list() == [
        1985000,
        1985000,
        4860000,
        4860000,
        7975000,
        13365000,
        24140000,
        24195000,
        24195000,
        24325000,
        24535000,
        24535000,
        28760000,
        28760000,
        44865000,
        50560000,
        81465000,
        81465000,
        158625000,
        161085000,
        168200000,
        168200000,
    ]
    assert result.get_column("type").sort().to_list() == [
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DEL",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
        "DUP",
    ]
    assert result.get_column("tool").sort().to_list() == ["wisecondor"] * 22
    assert result.get_column("sample").sort().to_list() == [
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E50",
        "31E51",
        "31E51",
        "31E51",
        "31E51",
        "31E51",
        "31E52",
        "31E52",
        "31E52",
        "31E52",
        "31E52",
        "31E52",
        "31E52",
        "31E52",
    ]
    assert result.get_column("gt").sort().to_list() == [
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        1,
        2,
        2,
        2,
    ]
    assert result.get_column("zs").sort().to_list() == [
        -86.94726190138441,
        -85.26031264460592,
        -73.58155536595004,
        -73.49114474905434,
        -50.95450266259022,
        -45.846312735339446,
        -44.925688917526934,
        -38.99657695343938,
        -19.229216003038786,
        -9.442566537678365,
        -5.545238215269585,
        -5.144791634472006,
        6.028755486524453,
        6.463537281517749,
        7.415727209306482,
        8.365696063453944,
        10.769387586443782,
        31.483716176165192,
        33.89310022563685,
        48.95626906157351,
        102.59442336756443,
        104.14024508530044,
    ]
    assert result.get_column("rt").sort().to_list() == [
        -8.39,
        -1.0405,
        -0.9997,
        -0.9988,
        -0.9808,
        -0.9803,
        -0.9773,
        -0.973,
        -0.9718,
        -0.9577,
        -0.9563,
        -0.9217,
        0.5418,
        0.5557,
        0.5651,
        0.5657,
        0.5842,
        0.593,
        0.5953,
        0.6063,
        0.9493,
        1.0042,
    ]


def test_pid_variant() -> None:
    """Check get variant of prescription."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_variant_of_prescription("AAAA")
    result = result.filter(polars.col("dp") > 70)

    assert result.get_column("id").sort().to_list() == [
        2866025335101587460,
        2866025335101587460,
        2866025335101587460,
        3577562291320651781,
        3577562291320651781,
        3577562291320651781,
        3953900094733942790,
    ]


def test_pid_variants() -> None:
    """Check get variant of prescription."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_variant_of_prescriptions(["EEEE", "FFFF"])
    result = result.filter(polars.col("dp") > 70)

    assert result.get_column("id").sort().to_list() == [
        374411351667245061,
        374411351667245061,
        440931590399328267,
        1488122785568915463,
        1488122785568915463,
        1488122785568915463,
        1776801427457310727,
        1776801427457310727,
        1776801427457310727,
        5395614109402136580,
        5888066656457981956,
        5888066656457981956,
        5888066656457981956,
        5981677259675664388,
        5981677259675664388,
        5984462191632318470,
        5984462191632318470,
        5984462191632318470,
        6099135273667395590,
        6099135273667395590,
    ]


def test_get_annotations() -> None:
    """Check get annotations."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    result = sake.get_annotations("snpeff", "4.3t")
    result = result.filter(polars.col("snpeff_impact") == "HIGH")

    assert result.get_column("id").sort().to_list() == []


def test_add_variants() -> None:
    """Check add variant."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    data = TRUTH.select("id", "id_part", "sample", "gt", "ad", "dp", "gq")
    result = sake.add_variants(data)

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "id_part", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_genotypes() -> None:
    """Check add genotype."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    variants = sake.get_interval("X", 47115191, 99009863)

    result = sake.add_genotypes(variants)

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)

    result = sake.add_genotypes(variants, keep_id_part=True)
    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "id_part", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)

    result = sake.add_genotypes(variants, drop_column=["gq"])
    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "sample", "gt", "ad", "dp")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_samples_info() -> None:
    """Check add samples_info."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    variants = sake.get_interval("X", 47115191, 99009863)
    genotyped = sake.add_genotypes(variants)

    samples_info = sake.add_sample_info(genotyped)

    truth = TRUTH.drop(
        "id_part",
        "father_gt",
        "mother_gt",
        "index_gt",
        "father_dp",
        "mother_dp",
        "index_dp",
        "father_gq",
        "mother_gq",
        "index_gq",
        "father_ad",
        "mother_ad",
        "index_ad",
        "origin",
        "snpeff_effect",
        "snpeff_impact",
        "snpeff_gene",
        "snpeff_geneid",
        "snpeff_feature",
        "snpeff_feature_id",
        "snpeff_bio_type",
        "snpeff_rank",
        "snpeff_hgvs_c",
        "snpeff_hgvs_p",
        "snpeff_cdna_pos",
        "snpeff_cdna_len",
        "snpeff_cds_pos",
        "snpeff_cvs_len",
        "snpeff_aa_pos",
    )
    result = samples_info

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_transmissions() -> None:
    """Check add transmissions."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    variants = sake.get_interval("X", 47115191, 99009863)
    genotyped = sake.add_genotypes(variants)
    samples_info = sake.add_sample_info(genotyped)

    transmissions = sake.add_transmissions(samples_info)

    truth = TRUTH.drop(
        "id_part",
        "snpeff_effect",
        "snpeff_impact",
        "snpeff_gene",
        "snpeff_geneid",
        "snpeff_feature",
        "snpeff_feature_id",
        "snpeff_bio_type",
        "snpeff_rank",
        "snpeff_hgvs_c",
        "snpeff_hgvs_p",
        "snpeff_cdna_pos",
        "snpeff_cdna_len",
        "snpeff_cds_pos",
        "snpeff_cvs_len",
        "snpeff_aa_pos",
    ).filter(polars.col("kindex"))
    result = transmissions

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_annotations() -> None:
    """Check add annotations."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path, "germline")

    variants = sake.get_interval("X", 47115191, 99009863)
    annotations = sake.add_annotations(
        variants,
        "snpeff",
        "4.3t",
        select_columns=[
            "effect",
            "impact",
            "gene",
            "geneid",
            "feature",
            "feature_id",
            "bio_type",
            "rank",
            "hgvs_c",
            "hgvs_p",
            "cdna_pos",
            "cdna_len",
            "cds_pos",
            "cvs_len",
            "aa_pos",
        ],
    )

    truth = TRUTH.select(
        "id",
        "chr",
        "pos",
        "ref",
        "alt",
        "snpeff_effect",
        "snpeff_impact",
        "snpeff_gene",
        "snpeff_geneid",
        "snpeff_feature",
        "snpeff_feature_id",
        "snpeff_bio_type",
        "snpeff_rank",
        "snpeff_hgvs_c",
        "snpeff_hgvs_p",
        "snpeff_cdna_pos",
        "snpeff_cdna_len",
        "snpeff_cds_pos",
        "snpeff_cvs_len",
        "snpeff_aa_pos",
    ).unique("id")

    polars.testing.assert_frame_equal(
        annotations,
        truth,
        check_row_order=False,
        check_column_order=False,
    )

    annotations = sake.add_annotations(
        variants,
        "snpeff",
        "4.3t",
        select_columns=[
            "effect",
            "impact",
            "gene",
            "geneid",
            "feature",
            "feature_id",
            "bio_type",
            "rank",
            "hgvs_c",
            "hgvs_p",
            "cdna_pos",
            "cdna_len",
            "cds_pos",
            "cvs_len",
            "aa_pos",
        ],
        rename_column=False,
    )

    truth = (
        TRUTH.select(
            "id",
            "chr",
            "pos",
            "ref",
            "alt",
            "snpeff_effect",
            "snpeff_impact",
            "snpeff_gene",
            "snpeff_geneid",
            "snpeff_feature",
            "snpeff_feature_id",
            "snpeff_bio_type",
            "snpeff_rank",
            "snpeff_hgvs_c",
            "snpeff_hgvs_p",
            "snpeff_cdna_pos",
            "snpeff_cdna_len",
            "snpeff_cds_pos",
            "snpeff_cvs_len",
            "snpeff_aa_pos",
        )
        .unique("id")
        .rename(lambda x: x.replace("snpeff_", ""))
    )

    polars.testing.assert_frame_equal(annotations, truth, check_row_order=False, check_column_order=False)
