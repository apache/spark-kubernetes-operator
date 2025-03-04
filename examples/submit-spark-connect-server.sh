#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

curl -XPOST http://localhost:6066/v1/submissions/create \
--data '{
  "appResource": "",
  "sparkProperties": {
    "spark.master": "spark://prod-master-svc:7077",
    "spark.submit.deployMode": "cluster",
    "spark.app.name": "SparkConnectServer",
    "spark.driver.cores": "1",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.executor.memory": "1g",
    "spark.cores.max": "2",
    "spark.ui.reverseProxy": "true"
  },
  "clientSparkVersion": "",
  "mainClass": "org.apache.spark.sql.connect.service.SparkConnectServer",
  "environmentVariables": { },
  "action": "CreateSubmissionRequest",
  "appArgs": [ ]
}'
