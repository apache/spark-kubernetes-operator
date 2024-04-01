<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Developer Guide

## Build Operator Locally

To build operator locally, use

```shell
./gradlew clean build
```

If you are working on API (CRD) changes, remember to update CRD yaml in chart as well

```shell
# This requires yq installed locally to add additional printer columns
# could be removed after fixing https://github.com/fabric8io/kubernetes-client/issues/3069
./gradlew :spark-operator-api:copyGeneratedCRD
```

## Build Operator Image Locally

   ```bash
   # Build a local container image which can be used for minikube.etc. 
   # For testing in remote k8s cluster, please also do `docker push` to make it available 
   # to the cluster / nodes 
   docker build --build-arg BASE_VERSION=1.0.0-alpha -t spark-kubernetes-operator:1.0.0-alpha .    
   ```

## Deploy Operator

### Install the Spark Operator

   ```bash
   helm install spark-kubernetes-operator \
      -f build-tools/helm/spark-kubernetes-operator/values.yaml \ 
       build-tools/helm/spark-kubernetes-operator/
   ```

### Upgrade the operator to a new version

   ```bash
   # update CRD as applicable
   kubectl replace -f /path/to/build-tools/helm/spark-kubernetes-operator/crds/sparkapplications.org.apache.spark-v1.yml
   
   # upgrade deployment 
   helm upgrade spark-kubernetes-operator \
      -f build-tools/helm/spark-kubernetes-operator/values.yaml \
      --set image.tag=<new_image_tag> \
       build-tools/helm/spark-kubernetes-operator/
   ```

## Run Tests

In addition to unit tests, we are actively working on the e2e test framework for the 
operator. This depends on the CI integration for operator.

For now, in order to manually run e2e tests:

* Build operator image and install the built image in k8s cluster
* Run AppSubmitToSucceedTest 

```shell
java -cp /path/to/spark-operator-test.jar \
    -Dspark.operator.test.app.yaml.files.dir=/path/to/e2e-tests/ \
    org.apache.spark.kubernetes.operator.AppSubmitToSucceedTest
```
