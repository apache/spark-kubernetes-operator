package org.apache.spark.k8s.operator.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

@EnableKubernetesMockClient
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class StatusRecorderTest {

  public static final String DEFAULT_NS = "default";
  KubernetesMockServer server;
  KubernetesClient client;

  SparkAppStatusListener mockStatusListener = mock(SparkAppStatusListener.class);

  StatusRecorder<ApplicationStatus, SparkApplication, SparkAppStatusListener> statusRecorder =
      new StatusRecorder<>(
          List.of(mockStatusListener), ApplicationStatus.class, SparkApplication.class);

  @Test
  void retriesFailedStatusPatches() {
    var testResource = TestUtils.createMockApp(DEFAULT_NS);
    testResource.getMetadata().setResourceVersion("1");
    var updated = TestUtils.createMockApp(DEFAULT_NS);
    updated.getMetadata().setResourceVersion("2");
    BaseContext<SparkApplication> context = mock(BaseContext.class);
    when(context.getResource()).thenReturn(testResource);
    when(context.getClient()).thenReturn(client);
    var path =
        "/apis/spark.apache.org/v1/namespaces/"
            + DEFAULT_NS
            + "/sparkapplications/"
            + testResource.getMetadata().getName()
            + "/status";
    server.expect().withPath(path).andReturn(500, null).once();
    server.expect().withPath(path).andReturn(200, updated).once();

    statusRecorder.persistStatus(context, new ApplicationStatus());

    verify(mockStatusListener, times(1))
        .listenStatus(
            assertArg(a -> assertThat(a.getMetadata().getResourceVersion()).isEqualTo("2")),
            any(),
            any());
  }
}
