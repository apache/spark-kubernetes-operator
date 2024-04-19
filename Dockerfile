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
#

FROM gradle:8.7-jdk17-alpine AS builder
ARG BASE_VERSION
WORKDIR /app
COPY . .
RUN ./gradlew clean build -x test

FROM eclipse-temurin:17-jre-jammy
ARG BASE_VERSION

ENV SPARK_OPERATOR_HOME=/opt/spark-operator
ENV SPARK_OPERATOR_WORK_DIR=/opt/spark-operator/operator
ENV BASE_VERSION=$BASE_VERSION
ENV OPERATOR_JAR=spark-kubernetes-operator-$BASE_VERSION-all.jar

WORKDIR $SPARK_OPERATOR_WORK_DIR

RUN groupadd --system --gid=9999 spark && \
    useradd --system --home-dir $SPARK_OPERATOR_HOME --uid=9999 --gid=spark spark

COPY --from=builder /app/spark-operator/build/libs/$OPERATOR_JAR .
COPY docker-entrypoint.sh .

RUN chown -R spark:spark $SPARK_OPERATOR_HOME && \
    chown spark:spark $OPERATOR_JAR && \
    chown spark:spark docker-entrypoint.sh

USER spark
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
