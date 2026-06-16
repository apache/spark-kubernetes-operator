# Security policy

## Reporting a vulnerability

To report a security vulnerability in apache/spark-kubernetes-operator, follow the ASF security process at <https://www.apache.org/security/>.  Reports can be sent privately to security@apache.org or to private@spark.apache.org.

## Threat model

This repository extend K8s resource manager to manage Apache Spark applications via Operator Pattern. Apache Spark's security guidance for operators — covering authentication, encryption, access control, Kerberos integration, trust boundaries, environmental assumptions, security properties provided and not provided, and downstream operator responsibilities — is documented at:

<https://spark.apache.org/docs/latest/security.html>

Automated security scanning agents should consult that document for the project's in-scope / out-of-scope declarations before reporting issues.

## Pod Security Admission

Cluster administrators should label workload namespaces with the desired
[Pod Security Admission](https://kubernetes.io/docs/concepts/security/pod-security-admission/) level.
The chosen level is enforced by the Kubernetes API server on every pod the operator launches,
whether or not a pod template is supplied. The pod template is, however, the only place to set the
pods' security context: Spark's default pods omit the fields that stricter profiles require (`runAsNonRoot`,
`allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile`), so running under a
stricter level requires supplying a pod template that carries them. A pod that does not meet the
enforced level is rejected by the API server, so the pods never start and the workload fails to
launch.

The operator passes these pod templates through unchanged. For a `SparkApplication`, the driver and
executor pods come from `.spec.driverSpec.podTemplateSpec` and `.spec.executorSpec.podTemplateSpec`,
mirroring Apache Spark's native `spark.kubernetes.[driver/executor].podTemplateFile` feature. For a
`SparkCluster`, the master and worker pods come from the StatefulSet pod templates
`.spec.masterSpec.statefulSetSpec.template` and `.spec.workerSpec.statefulSetSpec.template`.
