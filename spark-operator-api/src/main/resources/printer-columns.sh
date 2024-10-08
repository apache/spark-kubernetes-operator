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
for f in $(ls ${SCRIPT_PATH}/../../../build/classes/java/main/META-INF/fabric8/*.spark.apache.org-v1.yml); do
  yq -i '.spec.versions[0] += ({"additionalPrinterColumns": [{"jsonPath": ".status.currentState.currentStateSummary", "name": "Current State", "type": "string"}, {"jsonPath": ".metadata.creationTimestamp", "name": "Age", "type": "date"}]})' $f
done

