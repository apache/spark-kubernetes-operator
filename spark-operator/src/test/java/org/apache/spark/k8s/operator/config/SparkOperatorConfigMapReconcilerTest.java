package org.apache.spark.k8s.operator.config;

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.RECONCILER_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
@EnableKubeAPIServer
class SparkOperatorConfigMapReconcilerTest {

  public static final Long TARGET_RECONCILER_INTERVAL = 60L;

  private static KubernetesClient client;

  Operator operator;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void startController() {
    var reconciler =
        new SparkOperatorConfigMapReconciler(mock(Function.class), mock(Function.class));
    operator = new Operator(o -> o.withKubernetesClient(client));
    operator.register(reconciler);
    operator.start();
  }

  @AfterEach
  void stopController() {
    operator.stop();
  }

  @Test
  void sanityTest() {
    client.resource(testConfigMap()).create();

    await()
        .untilAsserted(
            () -> {
              assertThat(RECONCILER_INTERVAL_SECONDS.getValue()).isEqualTo(60L);
            });
    // adding this here to make pmd happy
    assertThat(RECONCILER_INTERVAL_SECONDS.getValue()).isEqualTo(60L);
  }

  ConfigMap testConfigMap() {
    ConfigMap configMap = new ConfigMap();
    configMap.setMetadata(
        new ObjectMetaBuilder().withName("spark-conf").withNamespace("default").build());
    configMap.setData(
        Map.of(RECONCILER_INTERVAL_SECONDS.getKey(), TARGET_RECONCILER_INTERVAL.toString()));
    return configMap;
  }
}
