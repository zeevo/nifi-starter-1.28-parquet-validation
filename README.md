# nifi-starter

A Maven monorepo for building Apache **NiFi 1.28.1** extensions: a controller service API,
its implementation, custom processors, and the NAR that packages them. Every example exists
in both **Java and Kotlin**.

Scaffolded with the official NiFi archetypes (`nifi-processor-bundle-archetype` and
`nifi-service-bundle-archetype`, both `1.28.1`), then merged into a single reactor.

## Layout

```
nifi-starter/
├── mise.toml                  JDK 11 + Maven pinned for this repo
├── pom.xml                    standalone parent: versions, dependencyManagement, nar plugin
├── nifi-starter-api/          jar  — StarterService (ControllerService interface)
├── nifi-starter-services/     jar  — StandardStarterService + KotlinStarterService
├── nifi-starter-processors/   jar  — TransformContentProcessor, KotlinTransformProcessor, ValidateParquet
├── nifi-starter-nar/          nar  — bundles all three for deployment
├── docker-compose.yml         single-node NiFi 1.28.1 for local testing
└── nars/                      staging dir mounted as NiFi's autoload directory
```

Interfaces live in their own module and implementations depend on it — ordinary Java
layering. The jar/NAR distinction is orthogonal: modules are *compile* units, a NAR is a
*deployment and classloader* unit.

```
nifi-starter-nar  (api + services + processors)
      └── parent: nifi-hadoop-libraries-nar  (Hadoop client libraries)
            └── parent: nifi-standard-shared-nar
                  └── parent: nifi-standard-services-api-nar  (NiFi's standard service APIs)
```

The parent link is a single `<type>nar</type>` dependency, written to the manifest as
`Nar-Dependency-Id`. A NAR may declare **at most one** NAR dependency, but parent NARs
chain: `nifi-hadoop-libraries-nar` supplies the Hadoop client jars that `ValidateParquet`'s
`parquet-hadoop` dependency needs at runtime (declared `provided` here, so never bundled),
and its own ancestry keeps `SSLContextService`, `DBCPService`, `RecordReaderFactory` and
friends on the classloader chain too. This mirrors how NiFi's own `nifi-parquet-nar` is
wired.

### When you'd need a separate API NAR

Everything ships in one NAR, so one classloader loads the interface and its implementation
and they trivially agree on types. If you ever split the service and the processors into
**separate NARs**, the interface must move into a third NAR that is an ancestor of both —
packaging the API jar into each NAR does not work.

The reason is that NiFi decides which services a property will accept by class identity
(`StandardControllerServiceProvider`):

```java
.filter(service -> serviceType.isAssignableFrom(service.getProxiedControllerService().getClass()))
```

Two NARs that each bundle the same API jar produce two distinct `Class` objects with the
same name, so `isAssignableFrom` is false and the service never appears in the dropdown.
This is exactly why NiFi ships `nifi-standard-services-api-nar` separately from the NARs
that implement and consume those APIs.

Note that moving the API into a different bundle later changes its bundle coordinates, which
existing flows referencing the service will notice.

## Prerequisites

[mise](https://mise.jdx.dev) provisions the exact JDK and Maven this repo expects:

```bash
brew install mise
mise install          # reads mise.toml -> Temurin 11.0.31, Maven 3.9.16
```

NiFi 1.28.1's own poms default to Java 8 bytecode (its released `nifi-api` jar is class file
major version 52) and bump to 11 under a JDK 11+ profile. This project pins
`maven.compiler.release` to 11 explicitly rather than depending on profile activation.
Without mise, any JDK 11–21 and Maven 3.9+ work.

## Build

```bash
mvn clean install       # compiles, runs tests, builds the NAR
mvn test                # tests only
```

Artifact: `nifi-starter-nar/target/nifi-starter-nar-1.0.0-SNAPSHOT.nar`

## Run it in NiFi

```bash
mvn clean install
cp nifi-starter-nar/target/*.nar nars/
docker compose up -d
```

Open <https://localhost:8443/nifi> (self-signed cert) and log in with `nifi` /
`nifinifinifinifi`.

To try the example: add a **TransformContentProcessor**, create a
**StandardStarterService** for its *Starter Service* property, enable the service, and feed
it a FlowFile. Content `hello` becomes `starter-hello`.

`nars/` is NiFi's autoload directory, so re-copying a rebuilt NAR redeploys it without a
container restart.

## What the example code does

| Class | Lang | Module | Role |
|---|---|---|---|
| `StarterService` | Java | api | `String transform(String)` — the service contract |
| `StandardStarterService` | Java | services | Prefixes values, optionally upper-cases them |
| `KotlinStarterService` | Kotlin | services | Same behaviour, default prefix `kotlin-` |
| `TransformContentProcessor` | Java | processors | Transforms content via the service |
| `KotlinTransformProcessor` | Kotlin | processors | Same, writes `starter.kotlin.service.id` |
| `ValidateParquet` | Java | processors | Routes FlowFiles by conformance to an expected Parquet schema |

### ValidateParquet

Validates FlowFile content against a known Parquet schema, given in Parquet message type
syntax (e.g. `message event { required int64 id; optional binary name (STRING); }`).
Conforming files route to `valid` (with a `record.count` attribute), everything else to
`invalid` (with the reason in `parquet.validation.detail`). Content is never modified.

- **Schema Attribute Name** (optional): a FlowFile attribute carrying its own expected
  schema, in the same syntax. When set and the attribute is present, that schema is used
  for that FlowFile; when the attribute is missing or blank, `Parquet Schema` is the
  fallback. An attribute that is present but malformed routes to `invalid` rather than
  falling back, so a broken schema is never mistaken for an absent one.
- **Schema Match Mode**: `Exact` (same columns, order, types, repetition) or `Contains`
  (file must contain every expected column; extras allowed). The top-level message name is
  ignored in both modes, since producers name it inconsistently (`spark_schema`, `root`,
  the Avro record name, ...).
- **Validation Depth**: `Schema and Content` (default) additionally decompresses and
  decodes every row, verifying page checksums, which catches data-page corruption that a
  footer check cannot see. `Schema Only` reads just the footer, which is orders of
  magnitude faster on large files.

Parquet reads a row group at a time, so memory for deep validation tracks row-group size
(commonly up to 128 MB), not file size. All parsing and decoding is upstream
`parquet-hadoop` (the same parquet-mr library NiFi's own parquet bundle uses); the only
custom I/O is a small `InputFile` adapter over the FlowFile stream, modeled on NiFi's
`NifiParquetInputFile`.

Extensions are discovered through `META-INF/services/` files — `org.apache.nifi.processor.Processor`
and `org.apache.nifi.controller.ControllerService`. **A new processor or service that isn't
listed there will not appear in NiFi**, regardless of which language it's written in.

Tests use `nifi-mock`'s `TestRunner`. Note that `nifi-starter-processors` tests against a
stub service (`StubStarterService`) rather than depending on the implementation module, so
the two stay decoupled.

## Kotlin

Kotlin is wired up in every module; a module simply needs a `src/main/kotlin` (or
`src/test/kotlin`) directory. Mixed sources compile in one pass — `kotlin-maven-plugin` runs
first and reads the Java sources for cross-references, then javac compiles the Java against
the resulting class files. That ordering is why the root pom disables maven-compiler-plugin's
`default-compile`/`default-testCompile` executions and re-adds them after the Kotlin plugin;
within a phase, Maven runs plugins in declaration order.

The two languages interoperate freely, and the examples deliberately demonstrate it:

- `KotlinStarterService` implements the **Java** `StarterService` interface.
- `KotlinTransformProcessor` accepts any `StarterService`, Java or Kotlin.
- `KotlinTransformProcessorTest` (Kotlin) drives `StubStarterService` (Java).
- `KotlinStarterServiceTest` (Kotlin) drives `ServiceTestProcessor` (Java).

Two Kotlin specifics worth copying:

- Property descriptors and relationships go in a `companion object` and are annotated
  `@JvmField`, so they are real static fields — what NiFi's reflection and Java callers
  expect. Without `@JvmField` they become getters on a `Companion` object.
- `kotlin-stdlib` is a normal compile dependency and is bundled into the NAR
  (`kotlin-stdlib-2.4.10.jar`), because NiFi ships no Kotlin runtime of its own.

`jvmTarget` follows `maven.compiler.release`, so Kotlin and Java both emit Java 11 bytecode.

## Adding a processor

1. Create the class in `nifi-starter-processors/src/main/{java,kotlin}`, extending
   `AbstractProcessor`.
2. Add its fully-qualified name to
   `nifi-starter-processors/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor`.
3. Add a `TestRunner` test.
4. `mvn clean install`.

## Adding a controller service

1. Put the interface in `nifi-starter-api`, extending `ControllerService`.
2. Implement it in `nifi-starter-services/src/main/{java,kotlin}`, extending
   `AbstractControllerService`.
3. Add the implementation's FQN to
   `nifi-starter-services/src/main/resources/META-INF/services/org.apache.nifi.controller.ControllerService`.
4. `mvn clean install`.

## Notes on this setup

- **The root pom deliberately does not inherit `org.apache.nifi:nifi-nar-bundles`**, which
  is what the archetypes generate. That parent's enforcer rule `RequireReleaseDeps` rejects
  any non-`org.apache.nifi` SNAPSHOT dependency in a `nar` module, so a `-SNAPSHOT`
  external bundle cannot build against it. The versions in `pom.xml` mirror NiFi 1.28.1's
  own (`nifi-nar-maven-plugin` 1.5.1, JUnit 5.11.1, slf4j 2.0.16, Surefire 3.5.2).
- `nifi-api` is managed at `provided` scope, matching `nifi-nar-bundles`. The framework
  loads it from its own classloader, so it must never be bundled into a NAR — including
  transitively via `nifi-utils`. `nifi-starter-api`, by contrast, is a normal compile
  dependency and *is* bundled: nothing outside this NAR provides it at runtime.
- `nifi-starter-nar` bundles a handful of JAXB jars (`jaxb-runtime`, `jakarta.xml.bind-api`,
  …). They arrive from `nifi-utils`'s JDK 11+ profile and are harmless.
- To release, set a non-SNAPSHOT version: `mvn versions:set -DnewVersion=1.0.0`.
