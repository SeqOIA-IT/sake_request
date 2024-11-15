# Sake Request

Offers an object and some function to help user to interogate sake.

It's a wrapper around duckdb and thriller functions, so if sake_request doesn't meet your needs, feel free to draw inspiration from it.

## Create request object

```
import pathlib
import sake

sake_path = pathlib.Path("/path/to/your/sake")

sake_db = sake.Sake(sake_path)
```

`sake_db` object store:
- path usefull for sake request
- number of thread could be use, by default it's set to value return by [`os.cpu_count()`](https://docs.python.org/3/library/os.html#os.cpu_count)
- if you want activate [tqdm](https://tqdm.github.io/) or not, by default not
- an object `db` to store duckdb connection

```
sake_db = sake.Sake(sake_path, threads=3, activate_tqdm=True, annotations_path="my_annotations")
```

This `sake_db` object use 3 thread, activate tqdm progress bar, and annotations path are `sake_path / "my_annotations"` instead of default value.

## Get variants from a genomic region

```
df = sake_db.get_interval("germline", 10, 329_034, 1_200_340)
```

`df` is a [polars.DataFrame](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) you can make conversion to and from pandas with [`to_pandas()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_pandas.html#polars.DataFrame.to_pandas) and [`from_pandas()`](https://docs.pola.rs/api/python/stable/reference/api/polars.from_pandas.html). The result contains `chr`, `pos`, `ref` and `alt` column that are the minimum to define a variant and also a `id` it's a sake almost unique variants id.

If you have multiple region you could run this:
```
target_chrs = ["1", "2", "3"]
target_start = [10_000, 40_232, 80_000]
target_stop = [199_232, 50_123, 800_000]

df = sake_db.get_intervals("germline", target_chrs, target_start, target_stop)
```

You can see `get_intervals` as just a loop of `get_interval`.

## Get variants from prescription

```
df = sake_db.get_variant_of_prescription("AAAA", "germline")
```

DataFrame contains all variants(id, chr, pos, …) and genotype (gt, ad, …) information of prescription AAAA in germline dataset.

## Get variants with a specific annotations

```
df = polars.read_parquet(sake_db.annotations_path / "clinvar" / "20241103" / "10.parquet")
df = df.filter(polars.col("CLNSIG") == "pathogenic")

df = sake_db.add_variants(df, "germline")
```

## Add genotypes to variants

Your dataframe must contains `id` column (see [variants](#get-variants-from-a-genomic-region)).

```
df = sake_db.add_genotypes(df, "germline")
```

Now `df` store variants with sample information and genotyping:
- gt: number of 1 in GT column in vcf, phasing and . information are lose
- ad: string that stop AD column in vcf
- db: DP column in vcf
- gq: GQ column in vcf

```
df = sake_db.add_genotypes(df, "germline", drop_column=["gq"])
```

This df store not store `gq` column if you didn't need a column add it in drop_column.

## Add annotations

```
df = sake_db.add_annotations(df, "gnomad", "3.1.2")
```

By default all column in annotation are prefixed by annotation name. It's likely that not all columns are of interest to you, use parameter `select_columns` to list columns of interest. Use original name not with prefix.

```
df = sake_db.add_annotations(df, "gnomad", "genomes.4.1", rename_column=False, select_columns=["AC"])
```

This call add to `df` a column AC from the gnomad annotations.

## Add sample information

Your data frame must contains `sample` column (see [genotypes](#add-genotypes-to-variants))

```
df = sake_db.sample_info(df)
```

You can select which column you want add in your dataframe

```
df = sake_db.sample_info(df, select_columns=["pid_crc"])
```

Result only contains new `pid_crc` column.

## Add transmission information

Transmission information are available only for germline information and for kindex sample. Your dataset must contains `pid_crc` column (see [sample information](#add-sample-information))

```
index_transmission = sake_db.add_transmissions(df)
```

Result contains only variant of kindex sample with genotype column for index sample, father and mother with coresponding prefix and an origin column. More details in how origin column are build in [variantplaner documentation](https://natir.github.io/variantplaner/usage/#compute-transmission-mode).
