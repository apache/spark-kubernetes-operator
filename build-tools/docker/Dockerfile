FROM gradle:8.9.0-jdk17-alpine AS builder
ARG APP_VERSION
WORKDIR /app
COPY . .
RUN ./gradlew clean build -x test

FROM azul/zulu-openjdk:21
ARG APP_VERSION
ARG SPARK_UID=185

ENV SPARK_OPERATOR_HOME=/opt/spark-operator
ENV SPARK_OPERATOR_WORK_DIR=/opt/spark-operator/operator
ENV APP_VERSION=$APP_VERSION
ENV OPERATOR_JAR=spark-kubernetes-operator-$APP_VERSION-all.jar

WORKDIR $SPARK_OPERATOR_WORK_DIR

RUN groupadd --system --gid=$SPARK_UID spark && \
    useradd --system --home-dir $SPARK_OPERATOR_HOME --uid=$SPARK_UID --gid=spark spark

COPY --from=builder /app/spark-operator/build/libs/$OPERATOR_JAR .
COPY --from=builder /app/build-tools/docker/docker-entrypoint.sh .

RUN chown -R spark:spark $SPARK_OPERATOR_HOME && \
    chown spark:spark $OPERATOR_JAR && \
    chown spark:spark docker-entrypoint.sh

USER spark
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
