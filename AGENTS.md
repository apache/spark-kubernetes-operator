# Agent Guide — Apache Spark K8s Operator

Guidance for AI agents and contributors working in this repository
(Apache Spark Kubernetes Operator).

## Project Overview

Apache Spark K8s Operator is a subproject of Apache Spark that extends Kubernetes to manage the
full lifecycle of Spark applications and clusters via the
[Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/). It watches
`SparkApplication` and `SparkCluster` custom resources and reconciles them — launching the
driver/master pods, observing executors/workers, and releasing resources on termination.

It is built on the [Java Operator SDK](https://javaoperatorsdk.io/) and the
[fabric8](https://fabric8.io/) Kubernetes client. The runtime entry point is
`org.apache.spark.k8s.operator.SparkOperator` in the `spark-operator` module.

See [docs/architecture.md](docs/architecture.md) for the reconciliation model and state machines,
and the rest of [docs/](docs/) for configuration and operations.

## Tech Stack & Requirements

The operator is implemented in **Java** and built with **Gradle** (multi-project,
configuration-cache enabled). It links against Apache Spark's Scala 2.13 artifacts but contains no
Scala source. Key runtime libraries:

- [Apache Spark](https://spark.apache.org/) `spark-core` / `spark-kubernetes` (Scala 2.13) and
  Hadoop — used to build driver/submission specs (compile-only in `spark-operator`, bundled into
  `spark-submission-worker`).
- [Java Operator SDK](https://javaoperatorsdk.io/) (`operator-framework`) — reconciliation runtime.
- [fabric8](https://fabric8.io/) `kubernetes-client` — Kubernetes API access and CRD generation.
- Log4j 2 + SLF4J for logging, Dropwizard Metrics for metrics, and Lombok for code generation.

Packaging and tooling: the Shadow plugin (operator fat jar), CycloneDX (SBOM), and the
Spotless / Checkstyle / PMD / SpotBugs / JaCoCo quality gate (see [Code Style](#code-style)).
**All dependency versions are pinned centrally in
[gradle/libs.versions.toml](gradle/libs.versions.toml)** — the single source of truth; bump versions
there, never inline in a module.

**Requirements**

- **JDK 21 or newer** — the Gradle toolchain auto-provisions JDK 26 and targets Java 21 bytecode;
  CI builds on Java 21 / 25 / 26.
- **Docker** — required for `./gradlew buildDockerImage`.
- **Kubernetes cluster** (e.g. minikube), **Helm 3**, and **kubectl** — to deploy and run the
  operator.
- **Chainsaw** (and **Go** to install it) — to run the `tests/e2e` suites.

## Module Layout

Gradle multi-project build (see [settings.gradle](settings.gradle)); root group is
`org.apache.spark.k8s.operator`, version `1.0.0-SNAPSHOT`.

- `spark-operator-api/` — CRD model: `SparkApplication` / `SparkCluster` spec, status, and diff
  types. Minimal dependencies (fabric8 `kubernetes-client` only); CRDs are generated here.
- `spark-submission-worker/` — Bridges to Spark's `spark-kubernetes` to build driver/submission
  specs. Depends on `spark-operator-api`.
- `spark-operator/` — The operator runtime: reconcilers, observers, metrics, health probes, config,
  and listeners. Shaded into a single fat jar via the Shadow plugin. Depends on the other two
  modules.
- `build-tools/docs-utils/` — Gradle subproject (`build-tools-docs-utils`) that generates the
  config and CRD documentation.
- `tests/e2e/` — [Chainsaw](https://kyverno.github.io/chainsaw/) end-to-end tests, grouped by
  scenario (e.g. `state-transition`, `python`, `watched-namespaces`); shared assertions live in
  `tests/e2e/assertions/`.
- `build-tools/`, `config/`, `examples/`, `docs/`, `dev/` — packaging (Docker / Helm), linter
  configs, example custom resources, documentation, and developer scripts.

## Build, Test & Dev Commands

Use `./gradlew` (the bundled Gradle wrapper). The build targets Java 21 bytecode while the
toolchain provisions JDK 26, so a JDK 21+ is required.

```bash
./gradlew build -x test          # Compile + run linters, skip tests (fast)
./gradlew build                  # Full build: compile, linters, and all unit tests
./gradlew :spark-operator:test   # Run a single module's tests
./gradlew :spark-operator:test --tests "org.apache.spark.k8s.operator.SparkOperatorTest"  # one class
./gradlew spotlessApply          # Auto-format sources (run before committing)
./gradlew spotlessCheck          # Verify formatting only
./gradlew javadoc                # Generate Javadoc (CI gate)
./gradlew buildDockerImage       # Build the apache/spark-kubernetes-operator:<version> image
./gradlew dependencyUpdates      # Report newer dependency versions
```

Helm chart (the lint mirrors CI):

```bash
helm lint --strict build-tools/helm/spark-kubernetes-operator
helm install spark -f build-tools/helm/spark-kubernetes-operator/values.yaml \
  build-tools/helm/spark-kubernetes-operator/
```

End-to-end tests need a running cluster (e.g. minikube) plus a freshly built image and Chainsaw:

```bash
chainsaw test --test-dir ./tests/e2e/state-transition --parallel 1
```

`./gradlew build` runs the full quality gate locally — Checkstyle, PMD, SpotBugs, Spotless, and
JaCoCo. Run it before pushing; CI runs the same matrix on Java 21 / 25 / 26 (x86 and arm).

## Code Style

- Java with 2-space indentation; max line length 100 (Checkstyle `severity=error`).
- Linter configs live under [config/](config/): Checkstyle (`config/checkstyle/checkstyle.xml`),
  PMD (`config/pmd/ruleset.xml`), SpotBugs (`config/spotbugs/spotbugs_exclude.xml`). Each fails the
  build on violation.
- Formatting is enforced by Spotless — run `./gradlew spotlessApply`. It trims trailing whitespace,
  removes unused imports, and ensures a trailing newline. Wrap a block in `// spotless:off` /
  `// spotless:on` to protect it.
- Import order, with a blank line between groups: `java`, `javax`, `scala`, everything else, then
  `org.apache.spark` last. Static imports come first.
- [Lombok](https://projectlombok.org/) is available (`@Slf4j`, `@Getter`, …) — prefer it over
  boilerplate, matching existing usage.
- Every non-Markdown source file needs the ASF Apache-2.0 license header (copy it from any existing
  `.java` / `.gradle` file). Markdown, `dev/`, and `.github/` are exempt — see
  [.github/.licenserc.yaml](.github/.licenserc.yaml). Markdown follows
  [.markdownlint.yaml](.markdownlint.yaml) (the MD013 line-length rule is disabled).

## Testing

- Unit tests use JUnit 5 (Jupiter) and Mockito; Kubernetes interactions are exercised with the
  fabric8 `kubernetes-server-mock` and the Java Operator SDK JUnit extension.
- Put tests in `src/test/java` mirroring the main package, and name classes `*Test`.
- Behavioral coverage lives in the Chainsaw specs under `tests/e2e/<group>/`.

## Commit & PR Guidelines

- Issues are tracked in the ASF JIRA project `SPARK`. Prefix every commit and PR title with the
  ticket: `[SPARK-XXXXX] Short imperative summary`
  (e.g. `[SPARK-57114] Upgrade spotless-plugin-gradle to 8.6.0`).
- PRs are merged via **squash** or **rebase** — no merge commits (see [.asf.yaml](.asf.yaml)). Keep
  changes surgical and scoped to the ticket.
- Run `./gradlew spotlessApply` and `./gradlew build` before opening a PR.
- Write PR descriptions in English following
  [.github/PULL_REQUEST_TEMPLATE](.github/PULL_REQUEST_TEMPLATE), and follow the
  [Spark contributing guide](https://spark.apache.org/contributing.html).

## Security

Security model: [SECURITY.md](./SECURITY.md)

Agents that scan this repository should consult `SECURITY.md` for the project's threat model, in-scope / out-of-scope declarations, and known non-findings before reporting issues.
