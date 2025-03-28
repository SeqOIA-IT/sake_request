"""Define duckdb query."""

__all__: list[str] = ["QUERY"]

QUERY: dict[str, str] = {
    "all_variants": """
    select
        v.id, v.chr, v.pos, v.ref, v.alt
    from
        read_parquet($path) as v
    """,
    "get_interval": """
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
    """,
    "get_variant_of_prescription": """
    select
        v.chr, v.pos, v.ref, v.alt, g.*
    from
        read_parquet($sample_path) as g
    join
        read_parquet($variant_path) as v
    on
        v.id = g.id
    """,
    "get_annotations": """
    select
        v.*, {columns}
    from
        read_parquet($annotation_path) as a
    join
        read_parquet($variant_path) as v
    on
        v.id = a.id
    """,
    "add_variants": """
    select
        v.chr, v.pos, v.ref, v.alt, d.*
    from
        read_parquet($path) as v
    join
        _data as d
    on
        v.id == d.id
    """,
    "add_annotations": """
    select
        v.*, {columns}
    from
        _data as v
    left join
        read_parquet($path) as a
    on
        v.id == a.id
    """,
    "add_sample_info": """
    select
        v.*, {columns}
    from
        _variants as v
    left join
        read_parquet($path) as s
    on
        v.sample == s.sample
    """,
    "add_transmissions": """
    select
        {columns}, t.*
    from
        _data as v
    left join
        read_parquet($path) as t
    on
        v.id == t.id
    where
        v.kindex == True
    """,
    "genotype_query": """
    select
        v.*, g.sample, g.gt, g.ad, g.dp, g.gq
    from
        _data as v
    left join
        read_parquet($path) as g
    on
        v.id == g.id
    """,
    "get_cnv": """
    select
        v.*
    from
        read_parquet($path) as v
    where
        v.start {start_comp} $start
    and
        v.end {stop_comp} $stop
    """,
}
