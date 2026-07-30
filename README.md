# nifi-starter

A Maven monorepo for building Apache **NiFi 1.28.1** extensions: a controller service API,
its implementation, custom processors, and the NAR that packages them.

Scaffolded with the official NiFi archetypes (`nifi-processor-bundle-archetype` and
`nifi-service-bundle-archetype`, both `1.28.1`), then merged into a single reactor.

## Layout

```
nifi-starter/
├── mise.toml                  JDK 11 + Maven pinned for this repo
├── pom.xml                    standalone parent: versions, dependencyManagement, nar plugin
├── nifi-starter-api/          jar  — StarterService (ControllerService interface)
├── nifi-starter-services/     jar  — StandardStarterService (implementation)
├── nifi-starter-processors/   jar  — TransformContentProcessor
├── nifi-starter-nar/          nar  — bundles all three for deployment
├── docker-compose.yml         single-node NiFi 1.28.1 for local testing
└── nars/                      staging dir mounted as NiFi's autoload directory
```

Interfaces live in their own module and implementations depend on it — ordinary Java
layering. The jar/NAR distinction is orthogonal: modules are *compile* units, a NAR is a
*deployment and classloader* unit.

```
nifi-starter-nar  (api + services + processors)
      └── parent: nifi-standard-services-api-nar  (NiFi's standard service APIs)
```

The parent link is a single `<type>nar</type>` dependency, written to the manifest as
`Nar-Dependency-Id`. A NAR may declare **at most one** NAR dependency. This project doesn't
currently use NiFi's standard services, but naming that NAR as the parent means a processor
here can reference `SSLContextService`, `DBCPService`, `RecordReaderFactory` and friends
without restructuring.

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

Java 11 matches what NiFi 1.28.1 is built against (`maven.compiler.release` is 11). Without
mise, any JDK 11–21 and Maven 3.9+ work.

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

| Class | Module | Role |
|---|---|---|
| `StarterService` | api | `String transform(String)` — the service contract |
| `StandardStarterService` | services | Prefixes values, optionally upper-cases them |
| `TransformContentProcessor` | processors | Reads content as text, transforms it via the service, writes it back |

Extensions are discovered through `META-INF/services/` files — `org.apache.nifi.processor.Processor`
and `org.apache.nifi.controller.ControllerService`. **A new processor or service that isn't
listed there will not appear in NiFi.**

Tests use `nifi-mock`'s `TestRunner`. Note that `nifi-starter-processors` tests against a
stub service (`StubStarterService`) rather than depending on the implementation module, so
the two stay decoupled.

## Adding a processor

1. Create the class in `nifi-starter-processors`, extending `AbstractProcessor`.
2. Add its fully-qualified name to
   `nifi-starter-processors/src/main/resources/META-INF/services/org.apache.nifi.processor.Processor`.
3. Add a `TestRunner` test.
4. `mvn clean install`.

## Adding a controller service

1. Put the interface in `nifi-starter-api`, extending `ControllerService`.
2. Implement it in `nifi-starter-services`, extending `AbstractControllerService`.
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
