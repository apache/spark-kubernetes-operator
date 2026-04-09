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
FROM gradle:9.4.1-jdk25-noble AS builder

WORKDIR /app

COPY . .

RUN --mount=type=cache,target=/home/gradle/.gradle/caches gradle --no-daemon clean build -x check

FROM azul/zulu-openjdk-alpine:26 AS jlink

COPY --from=builder /app/spark-operator/build/libs/spark-kubernetes-operator-*-all.jar /tmp/app.jar

RUN apk add --no-cache binutils && \
    MODULES=$(jdeps \
      --ignore-missing-deps \
      --print-module-deps \
      --multi-release 21 \
      /tmp/app.jar) && \
    MODULES="${MODULES},jdk.httpserver,jdk.unsupported,jdk.crypto.ec,jdk.security.auth,java.management" && \
    jlink \
      --add-modules ${MODULES} \
      --strip-debug \
      --compress zip-6 \
      --no-header-files \
      --no-man-pages \
      --output /opt/custom-jre

FROM alpine:3.23

ARG APP_VERSION
ARG SPARK_UID=185

LABEL org.opencontainers.image.authors="Apache Spark project <dev@spark.apache.org>"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.ref.name="Apache Spark Kubernetes Operator"
LABEL org.opencontainers.image.version="${APP_VERSION}"

ENV JAVA_HOME=/opt/custom-jre
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV SPARK_OPERATOR_HOME=/opt/spark-operator
ENV SPARK_OPERATOR_WORK_DIR=/opt/spark-operator/operator
ENV SPARK_OPERATOR_JAR=spark-kubernetes-operator.jar

WORKDIR $SPARK_OPERATOR_WORK_DIR

COPY --from=jlink /opt/custom-jre $JAVA_HOME

RUN apk add --no-cache libstdc++ && \
    addgroup -S -g $SPARK_UID spark && \
    adduser -S -h $SPARK_OPERATOR_HOME -u $SPARK_UID -G spark spark && \
    chown -R spark:spark $SPARK_OPERATOR_HOME

COPY --from=builder --chown=spark:spark /app/spark-operator/build/libs/spark-kubernetes-operator-*-all.jar $SPARK_OPERATOR_JAR
USER spark

CMD ["java", "-cp", "./spark-kubernetes-operator.jar", "org.apache.spark.k8s.operator.SparkOperator"]
