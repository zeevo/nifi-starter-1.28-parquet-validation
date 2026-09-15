# parquet-sdk

Runnable examples that write sample Parquet files with `parquet-java`, and print what each file
actually turned out to be. Written to be read: the point is the code and the output, not the files.

Nothing in the NAR depends on this module.

## Run it

With mise, which supplies the Java 11 and Maven this repo pins:

```bash
mise run samples
```

Or directly, if your toolchain is already right:

```bash
mvn compile exec:java@generate-sample-parquet-files -pl parquet-sdk
```

Files land in `parquet-sdk/target/samples`, which `mvn clean` removes. To write somewhere that
survives a clean, pass the output directory:

```bash
mise run samples -- -Dparquet.sdk.output=/tmp/samples
mvn compile exec:java@generate-sample-parquet-files -pl parquet-sdk -Dparquet.sdk.output=/tmp/samples
```

The `compile` is not optional. `exec:java` runs whatever is already in `target/classes`, so without
it you can be looking at the output of an older version of the code.

## What gets written

| file | shows |
| --- | --- |
| `01-primitives.parquet` | every Parquet physical type, all columns required |
| `02-nullable.parquet` | optional columns with nulls actually present |
| `03-nested.parquet` | records nested two deep, one optional and absent on a row |
| `04-collections.parquet` | an array, a map, and an array of records with a varying count per row |
| `05-logical-types.parquet` | date, timestamp, decimal, uuid and enum over primitives |
| `06-uncompressed.parquet` | 20,000 rows, no compression, as a size baseline |
| `07-snappy.parquet` | the same rows with SNAPPY |
| `08-gzip.parquet` | the same rows with GZIP |
| `09-zstd.parquet` | the same rows with ZSTD |
| `10-many-row-groups.parquet` | a 64 KiB row group target against the 128 MiB default |
| `11-dictionary-disabled.parquet` | the same rows as 07 with dictionary encoding off |
| `12-file-metadata.parquet` | custom key/value metadata in the footer |
| `13-empty.parquet` | a valid file with a schema and no rows |

## Things the output makes obvious

**Codecs are not close to each other.** The same 20,000 rows:

```
06-uncompressed   666,974 bytes
07-snappy         360,964 bytes   54%
08-gzip           196,795 bytes   29%
09-zstd           185,671 bytes   28%
```

**A nullable Avro field is a union, and that is what makes the Parquet column optional.**
Compare `01` (`required int64`) with `02` (`optional int64`), and note the `RLE` encoding that
appears in `02`: that is the definition levels recording which values are present.

**Records nest, to any depth.** A record is just a field type, so it composes. `03` is
`Customer -> Address -> Geo`, and Parquet stores each level as a group:

```
optional group address {
  required binary street (STRING);
  ...
  required group geo {
    required double latitude;
    required double longitude;
  }
}
```

Two things follow, both visible in the output. Leaf columns are named by their **full path**, so the
column is `address.geo.latitude`. And there is **no column for a group itself**: groups are
structure, only leaves hold data.

**An absent record is not a record full of nulls.** `address` in `03` is optional, and the row
preview shows both cases:

```
{"id": 2, ..., "address": {"street": "2 High Street", ..., "postcode": null, "geo": {...}}}
{"id": 3, ..., "address": null}
```

Parquet tells those apart with definition levels on the leaves underneath, which is why the nested
columns carry `RLE` encoding even where the leaf itself is required.

**Arrays of records are where nesting and repetition meet.** `04` has `items`, an array of
`LineItem`, with a different number per row. That produces `items.array.sku` and friends, and is the
shape most real data takes: an order with line items, a person with addresses.

**Logical types are annotations on a physical type.** In `05` a date is an `INT32 (DATE)`, a decimal
is a `BINARY (DECIMAL(9,2))`, and a uuid is a `FIXED_LEN_BYTE_ARRAY(16) (UUID)`.

**Avro's uuid does not reach Parquet by default.** It writes as a plain `STRING` unless
`parquet.avro.write-parquet-uuid` is set, which `SampleWriter.Options.parquetUuid(true)` does. This
one is easy to lose without noticing.

**Dictionary encoding is a per column decision, and it is visible.** In `07` the `category` column
reports `RLE_DICTIONARY` because it has four distinct values; `payload` does not, because every
value is unique. Turning dictionaries off in `11` changes the encodings and the size.

**Row groups only appear if you ask for them.** The writer's default target is 128 MiB, so a small
file is one row group no matter how many rows it holds. `10` lowers it to 64 KiB and the same rows
split many ways. Row groups are the unit a reader can skip or parallelise over.

**A zero row file is still a real file.** `13` has a schema, a footer and no data. Worth keeping
around, because plenty of code assumes at least one record exists.

## Where to look in the code

| class | for |
| --- | --- |
| `Schemas` | how each Avro schema is declared, and how it maps to Parquet |
| `SampleFiles` | one method per sample, each demonstrating one thing |
| `SampleWriter` | the `AvroParquetWriter` builder options, with notes on what each does |
| `ParquetSummary` | reading a footer back: schema, row groups, per column encodings, metadata, and a row preview for small files |
