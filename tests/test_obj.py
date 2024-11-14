"""Test sake submodule."""

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
            6409128687194603526,
            6409128687194603526,
            6409128687194603526,
            6448887235960897542,
            6448887235960897542,
            6448887235960897542,
            6450005477941051396,
            6450005477941051396,
            6450005477941051396,
            6474751846844989447,
            6474751846844989447,
            6474751846844989447,
            6482568290039234567,
            6482568290039234567,
            6490064296460943367,
            6490064296460943367,
        ],
        "chr": ["X"] * 16,
        "pos": [
            109481593,
            109481593,
            109481593,
            127995610,
            127995610,
            127995610,
            128516332,
            128516332,
            128516332,
            140039758,
            140039758,
            140039758,
            143679573,
            143679573,
            147170173,
            147170173,
        ],
        "ref": ["A", "A", "A", "C", "C", "C", "G", "G", "G", "T", "T", "T", "C", "C", "A", "A"],
        "alt": ["T", "T", "T", "T", "T", "T", "A", "A", "A", "G", "G", "G", "G", "G", "G", "G"],
        "id_part": [177, 177, 177, 178, 178, 178, 179, 179, 179, 179, 179, 179, 179, 179, 180, 180],
        "sample": [
            "CCC0",
            "CCC1",
            "CCC2",
            "DDD0",
            "DDD1",
            "DDD2",
            "DDD0",
            "DDD1",
            "DDD2",
            "AAA0",
            "AAA1",
            "AAA2",
            "AAA0",
            "AAA2",
            "AAA0",
            "AAA2",
        ],
        "gt": [2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 2],
        "ad": [
            "1,27",
            "0,43",
            "0,26",
            "0,29",
            "16,23",
            "0,22",
            "0,35",
            "0,30",
            "0,23",
            "0,45",
            "0,50",
            "0,33",
            "0,15",
            "0,51",
            "26,28",
            "0,30",
        ],
        "dp": [28, 43, 26, 29, 39, 22, 35, 30, 23, 45, 50, 33, 15, 51, 54, 30],
        "gq": [42, 99, 78, 87, 99, 66, 99, 90, 69, 99, 99, 99, 45, 99, 99, 90],
        "kindex": [
            True,
            False,
            False,
            True,
            False,
            False,
            True,
            False,
            False,
            True,
            False,
            False,
            True,
            False,
            True,
            False,
        ],
        "gender": ["F", "F", "M", "F", "F", "M", "F", "F", "M", "F", "F", "M", "F", "M", "F", "M"],
        "link": [
            "patient",
            "mere",
            "pere",
            "patient",
            "mere",
            "pere",
            "patient",
            "mere",
            "pere",
            "patient",
            "mere",
            "pere",
            "patient",
            "pere",
            "patient",
            "pere",
        ],
        "affected": [
            True,
            False,
            False,
            True,
            True,
            False,
            True,
            True,
            False,
            True,
            False,
            False,
            True,
            False,
            True,
            False,
        ],
        "pid_crc": [
            "CCCC",
            "CCCC",
            "CCCC",
            "DDDD",
            "DDDD",
            "DDDD",
            "DDDD",
            "DDDD",
            "DDDD",
            "AAAA",
            "AAAA",
            "AAAA",
            "AAAA",
            "AAAA",
            "AAAA",
            "AAAA",
        ],
        "preindication": ["p1"] * 16,
        "father_gt": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0],
        "mother_gt": [2, 2, 2, 1, 1, 1, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0],
        "index_gt": [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1],
        "father_dp": [26, 26, 26, 22, 22, 22, 23, 23, 23, 33, 33, 33, None, None, None, None],
        "mother_dp": [43, 43, 43, 39, 39, 39, 30, 30, 30, 50, 50, 50, None, None, None, None],
        "index_dp": [28, 28, 28, 29, 29, 29, 35, 35, 35, 45, 45, 45, 15, 15, 54, 54],
        "father_gq": [78, 78, 78, 66, 66, 66, 69, 69, 69, 99, 99, 99, None, None, None, None],
        "mother_gq": [99, 99, 99, 99, 99, 99, 90, 90, 90, 99, 99, 99, None, None, None, None],
        "index_gq": [42, 42, 42, 87, 87, 87, 99, 99, 99, 99, 99, 99, 45, 45, 99, 99],
        "father_ad": [
            "0,26",
            "0,26",
            "0,26",
            "0,22",
            "0,22",
            "0,22",
            "0,23",
            "0,23",
            "0,23",
            "0,33",
            "0,33",
            "0,33",
            None,
            None,
            None,
            None,
        ],
        "mother_ad": [
            "0,43",
            "0,43",
            "0,43",
            "16,23",
            "16,23",
            "16,23",
            "0,30",
            "0,30",
            "0,30",
            "0,50",
            "0,50",
            "0,50",
            None,
            None,
            None,
            None,
        ],
        "index_ad": [
            "1,27",
            "1,27",
            "1,27",
            "0,29",
            "0,29",
            "0,29",
            "0,35",
            "0,35",
            "0,35",
            "0,45",
            "0,45",
            "0,45",
            "0,15",
            "0,15",
            "26,28",
            "26,28",
        ],
        "origin": [
            "###",
            "###",
            "###",
            '#"#',
            '#"#',
            '#"#',
            "###",
            "###",
            "###",
            "###",
            "###",
            "###",
            "#!!",
            "#!!",
            '"!!',
            '"!!',
        ],
        "snpeff_effect": [
            "intron_variant",
            "intron_variant",
            "intron_variant",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intragenic_variant",
            "intragenic_variant",
            "intragenic_variant",
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
        ],
        "snpeff_gene": [
            "GUCY2F",
            "GUCY2F",
            "GUCY2F",
            "Z75741.1-ACTRT1",
            "Z75741.1-ACTRT1",
            "Z75741.1-ACTRT1",
            "AL442647.1",
            "AL442647.1",
            "AL442647.1",
            "HNRNPA3P3-RN7SL727P",
            "HNRNPA3P3-RN7SL727P",
            "HNRNPA3P3-RN7SL727P",
            "HNRNPH1P2-SPANXN2",
            "HNRNPH1P2-SPANXN2",
            "Z99497.2-AL589669.1",
            "Z99497.2-AL589669.1",
        ],
        "snpeff_geneid": [
            "ENSG00000101890",
            "ENSG00000101890",
            "ENSG00000101890",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000225689",
            "ENSG00000225689",
            "ENSG00000225689",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000213924-ENSG00000268988",
            "ENSG00000213924-ENSG00000268988",
            "ENSG00000271660-ENSG00000284377",
            "ENSG00000271660-ENSG00000284377",
        ],
        "snpeff_feature": [
            "transcript",
            "transcript",
            "transcript",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "gene_variant",
            "gene_variant",
            "gene_variant",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
            "intergenic_region",
        ],
        "snpeff_feature_id": [
            "ENST00000218006.2",
            "ENST00000218006.2",
            "ENST00000218006.2",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000224548-ENSG00000123165",
            "ENSG00000225689",
            "ENSG00000225689",
            "ENSG00000225689",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000214653-ENSG00000241248",
            "ENSG00000213924-ENSG00000268988",
            "ENSG00000213924-ENSG00000268988",
            "ENSG00000271660-ENSG00000284377",
            "ENSG00000271660-ENSG00000284377",
        ],
        "snpeff_bio_type": [
            "protein_coding",
            "protein_coding",
            "protein_coding",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ],
        "snpeff_rank": ["1/19", "1/19", "1/19", "", "", "", "", "", "", "", "", "", "", "", "", ""],
        "snpeff_hgvs_c": [
            "c.-86+273T>A",
            "c.-86+273T>A",
            "c.-86+273T>A",
            "n.127995610C>T",
            "n.127995610C>T",
            "n.127995610C>T",
            "n.128516332C>T",
            "n.128516332C>T",
            "n.128516332C>T",
            "n.140039758T>G",
            "n.140039758T>G",
            "n.140039758T>G",
            "n.143679573C>G",
            "n.143679573C>G",
            "n.147170173A>G",
            "n.147170173A>G",
        ],
        "snpeff_hgvs_p": [""] * 16,
        "snpeff_cdna_pos": [""] * 16,
        "snpeff_cdna_len": [""] * 16,
        "snpeff_cds_pos": [""] * 16,
        "snpeff_cvs_len": [""] * 16,
        "snpeff_aa_pos": [""] * 16,
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

    database = Sake(sake_path)

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(os.cpu_count())

    assert database.aggregations_path == sake_path / "aggregations"
    assert database.annotations_path == sake_path / "annotations"
    assert database.partitions_path == sake_path / "{target}" / "genotypes" / "partitions"
    assert database.samples_path == sake_path / "samples" / "patients.parquet"
    assert database.transmissions_path == sake_path / "{target}" / "genotypes" / "transmissions"
    assert database.variants_path == sake_path / "{target}" / "variants.parquet"


def test_set_value() -> None:
    """Check default object."""
    sake_path = pathlib.Path("sake")

    database = Sake(
        sake_path,
        threads=23,
        aggregations_path=sake_path / "other",
        annotations_path=sake_path / "other",
        partitions_path=sake_path / "other",
        samples_path=sake_path / "other",
        transmissions_path=sake_path / "other",
        variants_path=sake_path / "other",
    )

    assert database.sake_path == sake_path

    assert os.environ["POLARS_MAX_THREADS"] == str(23)

    assert database.aggregations_path == sake_path / "other"
    assert database.annotations_path == sake_path / "other"
    assert database.partitions_path == sake_path / "other"
    assert database.samples_path == sake_path / "other"
    assert database.transmissions_path == sake_path / "other"
    assert database.variants_path == sake_path / "other"


def test_extract_variants() -> None:
    """Check get interval."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    result = sake.get_interval("germline", "X", 47115191, 99009863)

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt").unique("id")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_variants() -> None:
    """Check add variant."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    data = TRUTH.select("id", "id_part", "sample", "gt", "ad", "dp", "gq")
    result = sake.add_variants(data, "germline")

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "id_part", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_genotypes() -> None:
    """Check add genotype."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "X", 47115191, 99009863)

    result = sake.add_genotypes(variants, "germline")

    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)

    result = sake.add_genotypes(variants, "germline", keep_id_part=True)
    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "id_part", "sample", "gt", "ad", "dp", "gq")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)

    result = sake.add_genotypes(variants, "germline", drop_column=["gq"])
    truth = TRUTH.select("id", "chr", "pos", "ref", "alt", "sample", "gt", "ad", "dp")

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_samples_info() -> None:
    """Check add samples_info."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "X", 47115191, 99009863)
    genotyped = sake.add_genotypes(variants, "germline")
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
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "X", 47115191, 99009863)
    genotyped = sake.add_genotypes(variants, "germline")
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
    )
    result = transmissions

    polars.testing.assert_frame_equal(result, truth, check_row_order=False, check_column_order=False)


def test_add_annotations() -> None:
    """Check add annotations."""
    sake_path = pathlib.Path("tests/data")
    sake = Sake(sake_path)

    variants = sake.get_interval("germline", "X", 47115191, 99009863)
    annotations = sake.add_annotations(
        variants,
        "snpeff",
        "4.3t/germline",
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

    polars.testing.assert_frame_equal(annotations, truth, check_row_order=False, check_column_order=False)

    annotations = sake.add_annotations(
        variants,
        "snpeff",
        "4.3t/germline",
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
    ).unique("id").rename(lambda x: x.replace("snpeff_", ""))

    polars.testing.assert_frame_equal(annotations, truth, check_row_order=False, check_column_order=False)
