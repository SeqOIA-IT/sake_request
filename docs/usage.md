# Sake Request

Is a python package that offers an API to help user to interogate sake.

It's a wrapper around duckdb and thriller functions, so if sake_request doesn't meet your needs, feel free to draw inspiration from it.

A Seqoia dAta laKE should look like this:
```
sake
|-- annotations
|   |-- clinvar
|   |-- …
|   |-- gnomad
|   |-- snpeff
|   `-- spliceai
|-- preindication_1
|   |-- genotypes
|   |   |-- partitions
|   |   |   |-- id_part=0
|   |   |   |-- …
|   |   |   `-- id_part=255
|   |   |-- samples
|   |   |   |-- sample_1.parquet
|   |   |   |-- …
|   |   |   `-- sample_Z.parquet
|   |   `-- transmissions
|   |       |-- sample_1.parquet
|   |       |-- …
|   |       `-- sample_Z.parquet
|   `-- variants
|-- preindication_2
|   |-- genotypes
|   |   |-- partitions
|   |   |   |-- id_part=0
|   |   |   |-- …
|   |   |   `-- id_part=255
|   |   |-- samples
|   |   |   |-- sample_1.parquet
|   |   |   |-- …
|   |   |   `-- sample_Z.parquet
|   |   `-- transmissions
|   |       |-- sample_1.parquet
|   |       |-- …
|   |       `-- sample_Z.parquet
|   `-- variants
`-- samples
   |-- patients.json
    |-- patients.parquet
    `-- pedigree
```

## Create request object

```
import pathlib
import sake

sake_path = pathlib.Path("/path/to/your/sake")

sake_db = sake.Sake(sake_path, "preindication_1")
```

`sake_db` object store:
- path usefull for sake request
- on which preindication your request are run
- number of thread could be use, by default it's set to value return by [`os.cpu_count()`](https://docs.python.org/3/library/os.html#os.cpu_count)
- if you want activate [tqdm](https://tqdm.github.io/) or not, by default not
- an object `db` to store duckdb connection

```
sake_db = sake.Sake(
    # mandatory argument
    sake_path,
	preindication,
	# optional argument
	threads=3,
	activate_tqdm=True,
	# overwrite annotations_path
	annotations_path="my_annotations"
)
```

This `sake_db` object use 3 thread, activate tqdm progress bar, and annotations path are `sake_path / "my_annotations"` instead of default value.

## Get variants from a genomic region

```
df = sake_db.get_interval(10, 329_034, 1_200_340)
```

`df` is a [polars.DataFrame](https://docs.pola.rs/api/python/stable/reference/dataframe/index.html) you can make conversion to and from pandas with [`to_pandas()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.to_pandas.html#polars.DataFrame.to_pandas) and [`from_pandas()`](https://docs.pola.rs/api/python/stable/reference/api/polars.from_pandas.html). The result contains `chr`, `pos`, `ref` and `alt` column that are the minimum to define a variant and also a `id` it's a sake almost unique variants id.

If you have multiple region you could run this:
```
target_chrs = ["1", "2", "3"]
target_start = [10_000, 40_232, 80_000]
target_stop = [199_232, 50_123, 800_000]

df = sake_db.get_intervals(
    "germline",
	target_chrs,
	target_start,
	target_stop
)
```

You can see `get_intervals` as just a loop of `get_interval`.

## Get variants from prescription

```
df = sake_db.get_variant_of_prescription("AAAA", "germline")
```

DataFrame contains all variants(id, chr, pos, …) and genotype (gt, ad, …) information of prescription AAAA in germline dataset.

## Get variants from an annotations

```
df = sake_db.get_annotations("clinvar", "20241103", "germline")
```

DataFrame contains all variants(id, chr, pos, …) and annotations information. By default columns are rename with annotations name as prefix, add `rename_column=False` in call to change this behavior. If you want just some column use `select_columns` parameter, use original name without prefix.

## Add variants to a dataframe

Your dataframe must contains `id` column (see [variants](#get-variants-from-a-genomic-region)).

```
df = sake_db.add_variants(df)
```

Now `df` store variants imformation:
- chr: chromosome name
- pos: position of variant
- ref: reference sequence
- alt: alternative sequence

## Add genotypes to variants

Your dataframe must contains `id` column (see [variants](#get-variants-from-a-genomic-region)).

```
df = sake_db.add_genotypes(df)
```

Now `df` store variants with sample information and genotyping:
- gt: number of 1 in GT column in vcf, phasing and . information are lose
- ad: string that stop AD column in vcf
- db: DP column in vcf
- gq: GQ column in vcf

```
df = sake_db.add_genotypes(df, drop_column=["gq"])
```

This df store not store `gq` column if you didn't need a column add it in drop_column.

### Other parameter

To add genotypes information sake_request add a column call `id_part` it's indicate in which genome block genotypes of variants are store. By default this column are drop if you want keep it set `keep_id_part` to True.

In sake structure example number of `id_part` are between 0 to 255 ($2^8 - 1$), but you could use more or less partition ([check variantplaner doc](https://seqoia-it.github.io/variantplaner/usage/#genotypes-structuration)). Number of partitions is a power of 2, `number_of_bits` parameter let you indicate how many partitions is use, default value are 8 $2^8 - 1$ are 255.

You could say to `add_genotypes` to read many partitions file in same time, with `read_threads` parameter.

```
df = sake_db.add_genotypes(
	df,
	keep_id_part: bool = False,
    drop_column: list[str] | None = None,
    number_of_bits: int = 8,
    read_threads: int = 1,
)
```

## Add annotations

```
df = sake_db.add_annotations(df, "gnomad", "3.1.2")
```

By default all column in annotation are prefixed by annotation name. It's likely that not all columns are of interest to you, use parameter `select_columns` to list columns of interest. Use original name not with prefix.

```
df = sake_db.add_annotations(
    df,
	"gnomad",
	"genomes.4.1",
	rename_column=False,
	select_columns=["AC"]
)
```

This call add to `df` a column AC from the gnomad annotations.

### Special case

Due to some specificity in annotations database some change are made automaticly on parameter:
- if database_name is `snpeff` or `variant2gene`, preindication is add after version value
- if database_name is `spliceai`, version value are ignore

In fact `add_annotations` method just concat `sake` path, `database_name` and `database_version`. So to add annotations just check path like `{sake.path}/{database_name}/{database_version}` contains parquet file for each chromosome.


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

```
index_transmission = sake_db.add_transmissions(
	df,
	drop_column=["mother_gq"]
)
```

This df store not store `mother_gq` column if you didn't need a column add it in drop_column.

You could say to `add_transmissions` to read many partitions file in same time, with `read_threads` parameter.

```
add_transmissions(
	df,
    drop_column: list[str] | None = None,
    read_threads: int = 1,
) -> DataFrame
```
