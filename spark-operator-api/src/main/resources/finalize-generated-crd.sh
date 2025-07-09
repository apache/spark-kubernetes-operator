#!/usr/bin/env bash

#
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
#

# This is a workaround. See https://github.com/fabric8io/kubernetes-client/issues/3069
# We do a yq to add printer columns

SCRIPT_PATH=$(cd "$(dirname "$0")"; pwd)
supported_versions_file="${SCRIPT_PATH}/supported-crd-versions.yml"
for f in $(ls ${SCRIPT_PATH}/../../../build/classes/java/main/META-INF/fabric8/*.spark.apache.org-v1.yml); do
  yq -i '.spec.versions[0] += ({"additionalPrinterColumns": [{"jsonPath": ".status.currentState.currentStateSummary", "name": "Current State", "type": "string"}, {"jsonPath": ".metadata.creationTimestamp", "name": "Age", "type": "date"}]})' $f
  filename=$(basename "$f")
  template_file_path="${SCRIPT_PATH}/../../../../build-tools/crd-templates/${filename}"
  target_dir="${SCRIPT_PATH}/../../../../build-tools/helm/spark-kubernetes-operator/crds"
  mkdir -p "${target_dir}"
  target_file_path="${target_dir}/${filename}"
  yq eval-all '
    select(fileIndex == 2) as $generatedSchema
    | select(fileIndex == 1) as $supportedVersionsFile
    | select(fileIndex == 0)
    | .spec.versions += (
        $supportedVersionsFile.values
        | map(
          $generatedSchema.spec.versions[0] as $tmpl
           | $tmpl * { "name": .}
          )
       )
  ' "$template_file_path" "$supported_versions_file" "$f" > "$target_file_path"
done

