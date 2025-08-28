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
FROM gradle:9.0.0-jdk17-noble AS builder

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/home/gradle/.gradle/caches gradle --no-daemon clean build -x check

FROM azul/zulu-openjdk:24-jre

ARG APP_VERSION=0.5.0-SNAPSHOT
ARG SPARK_UID=185

LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Kubernetes Operator"
LABEL org.opencontainers.image.version="${APP_VERSION}"

ENV SPARK_OPERATOR_HOME=/opt/spark-operator
ENV SPARK_OPERATOR_WORK_DIR=/opt/spark-operator/operator
ENV SPARK_OPERATOR_JAR=spark-kubernetes-operator.jar

WORKDIR $SPARK_OPERATOR_WORK_DIR

RUN groupadd --system --gid=$SPARK_UID spark && \
    useradd --system --home-dir $SPARK_OPERATOR_HOME --uid=$SPARK_UID --gid=spark spark && \
    chown -R spark:spark $SPARK_OPERATOR_HOME

COPY --from=builder --chown=spark:spark /app/spark-operator/build/libs/spark-kubernetes-operator-$APP_VERSION-all.jar $SPARK_OPERATOR_JAR
COPY --from=builder --chown=spark:spark /app/build-tools/docker/docker-entrypoint.sh .

USER spark

ENTRYPOINT ["/opt/spark-operator/operator/docker-entrypoint.sh"]

CMD ["help"]
