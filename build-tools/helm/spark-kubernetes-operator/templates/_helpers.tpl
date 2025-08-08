# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

{{/*
Expand the name of the chart.
*/}}
{{- define "spark-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "spark-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "spark-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Common labels
*/}}
{{- define "spark-operator.commonLabels" -}}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ include "spark-operator.chart" . }}
spark-role: operator
{{- end }}

{{/*
Dynamic config (hot properties) labels
*/}}
{{- define "spark-operator.dynamicConfigLabels" -}}
app.kubernetes.io/name: {{ include "spark-operator.name" . }}
app.kubernetes.io/component: "operator-dynamic-config-overrides"
{{- include "spark-operator.commonLabels" . }}
{{- end }}

{{/*
Bootstrap config labels
*/}}
{{- define "spark-operator.configLabels" -}}
app.kubernetes.io/name: {{ include "spark-operator.name" . }}
app.kubernetes.io/component: "operator-config"
{{- include "spark-operator.commonLabels" . }}
{{- end }}

{{/*
Deployment selector labels
*/}}
{{- define "spark-operator.deploymentSelectorLabels" -}}
app.kubernetes.io/name: {{ include "spark-operator.name" . }}
app.kubernetes.io/component: "operator-deployment"
{{- end }}

{{/*
Create the path of the operator image to use
*/}}
{{- define "spark-operator.imagePath" -}}
{{- if .Values.image.digest }}
{{- .Values.image.repository }}@{{ .Values.image.digest }}
{{- else }}
{{- .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end }}
{{- end }}

{{/*
List of Spark workload namespaces. If not provied in values, use the same namespace as operator
*/}}
{{- define "spark-operator.workloadNamespacesStr" -}}
{{- if index (.Values.workloadResources.namespaces) "data" }}
{{- $ns_list := join "," .Values.workloadResources.namespaces.data }}
{{- printf "%s" $ns_list }}
{{- else }}
{{- printf "%s" .Release.Namespace }}
{{- end }}
{{- end }}

{{/*
Default property overrides
*/}}
{{- define "spark-operator.defaultPropertyOverrides" -}}
# Runtime resolved properties
spark.kubernetes.operator.namespace={{ .Release.Namespace }}
spark.kubernetes.operator.name={{- include "spark-operator.name" . }}
spark.kubernetes.operator.dynamicConfig.enabled={{ .Values.operatorConfiguration.dynamicConfig.enable }}
spark.kubernetes.operator.metrics.port={{ include "spark-operator.metricsPort" . }}
spark.kubernetes.operator.health.probePort={{ include "spark-operator.probePort" . }}
{{- if .Values.workloadResources.namespaces.overrideWatchedNamespaces }}
spark.kubernetes.operator.watchedNamespaces={{ include "spark-operator.workloadNamespacesStr" . | trim }}
{{- end }}
{{- end }}

{{/*
Readiness Probe properties overrides
*/}}
{{- define "spark-operator.readinessProbe.failureThreshold" -}}
{{- default 30 .Values.operatorDeployment.operatorPod.operatorContainer.probes.startupProbe.failureThreshold }}
{{- end }}
{{- define "spark-operator.readinessProbe.periodSeconds" -}}
{{- default 10 .Values.operatorDeployment.operatorPod.operatorContainer.probes.startupProbe.periodSeconds }}
{{- end }}

{{/*
Liveness Probe properties override
*/}}
{{- define "spark-operator.livenessProbe.initialDelaySeconds" -}}
{{- default 30 .Values.operatorDeployment.operatorPod.operatorContainer.probes.livenessProbe.initialDelaySeconds }}
{{- end }}
{{- define "spark-operator.livenessProbe.periodSeconds" -}}
{{- default 10 .Values.operatorDeployment.operatorPod.operatorContainer.probes.livenessProbe.periodSeconds }}
{{- end }}

{{/*
Readiness Probe property overrides
*/}}
{{- define "spark-operator.probePort" -}}
{{- default 19091 .Values.operatorDeployment.operatorPod.operatorContainer.probes.port }}
{{- end }}

{{/*
Port for metrics
*/}}
{{- define "spark-operator.metricsPort" -}}
{{- default 19090 .Values.operatorDeployment.operatorPod.operatorContainer.metrics.port }}
{{- end }}
