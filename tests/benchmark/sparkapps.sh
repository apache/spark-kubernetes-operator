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

# 1. Clear the existing CRDs before staring the benchmark
echo "CLEAN UP NAMESPACE FOR BENCHMARK"
kubectl delete sparkapplications.spark.apache.org --all

NUM="${1:-1000}"
echo "START BENCHMARK WITH $NUM JOBS"

# 2. Creation Benchmark
filename=$(mktemp)

for i in $(seq -f "%05g" 1 $NUM); do
  cat << EOF >> $filename
apiVersion: spark.apache.org/v1
kind: SparkApplication
metadata:
  name: test-${i}
spec:
  mainClass: "org.apache.spark.examples.DriverSubmissionTest"
  jars: "local:///opt/spark/examples/jars/spark-examples.jar"
  driverArgs: ["0"]
  sparkConf:
    spark.driver.memory: "256m"
    spark.driver.memoryOverhead: "0m"
    spark.kubernetes.authenticate.driver.serviceAccountName: "spark"
    spark.kubernetes.container.image: "apache/spark:{{SPARK_VERSION}}-scala"
    spark.kubernetes.driver.master: "local[1]"
    spark.kubernetes.driver.pod.excludedFeatureSteps: "org.apache.spark.deploy.k8s.features.KerberosConfDriverFeatureStep"
    spark.kubernetes.driver.request.cores: "100m"
  runtimeVersions:
    sparkVersion: "4.1.1"
---
EOF
done

start=`date +%s`
kubectl apply -f $filename > /dev/null
while [ $(kubectl get sparkapplications.spark.apache.org | grep ResourceReleased | wc -l) -lt $NUM ]
do
  sleep 1
done
end=`date +%s`
completionTime=$((end - start))
echo "FINISHED $NUM JOBS IN $completionTime SECONDS."

# 3. Deletion Benchmark
start=`date +%s`
kubectl delete sparkapplications.spark.apache.org --all > /dev/null
end=`date +%s`
deletionTime=$((end - start))
echo "DELETED $NUM JOBS IN $deletionTime SECONDS."
