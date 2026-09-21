/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.k8s.operator.utils;

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.RECONCILER_FOREGROUND_REQUEST_TIMEOUT_SECONDS;
import static org.apache.spark.k8s.operator.utils.ModelUtils.buildOwnerReferenceTo;
import static org.apache.spark.k8s.operator.utils.SparkExceptionUtils.isConflictForExistingResource;

import java.security.cert.CertPathBuilderException;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;

/** Utility class for reconciler operations. */
@Slf4j
public final class ReconcilerUtils {

  /**
   * The code fabric8 reports whenever no response status was received. It is the sentinel of
   * {@link KubernetesClientException#getCode()}, not an HTTP status, and it covers three unrelated
   * cases: a connection that broke on its way to the API server, which carries the failure that
   * broke it; a rejection the client raised before sending the request, such as a missing name or
   * resource version, which carries no cause at all; and a response that arrived but could not be
   * parsed, which carries the parsing failure. Only the first heals on its own, see
   * {@link #brokeOnTheWayToTheApiServer}.
   */
  private static final int NO_RESPONSE_CODE = -1;

  /** Maximum number of links followed when looking for the cause that broke a request. */
  private static final int MAX_CAUSE_DEPTH = 10;

  private ReconcilerUtils() {}

  /**
   * Converts a ReconcileProgress to an UpdateControl.
   *
   * @param resource The resource being reconciled.
   * @param reconcileProgress The ReconcileProgress object.
   * @param <S> The type of the status.
   * @param <T> The type of the spec.
   * @param <O> The type of the resource, extending BaseResource.
   * @return An UpdateControl object.
   */
  public static <S, T, O extends BaseResource<?, ?, ?, ?, ?>> UpdateControl<O> toUpdateControl(
      O resource, ReconcileProgress reconcileProgress) {
    // reconciler already handled resource and status update, skip update at lower level
    UpdateControl<O> updateControl = UpdateControl.noUpdate();
    if (reconcileProgress.isRequeue()) {
      return updateControl.rescheduleAfter(reconcileProgress.getRequeueAfterDuration());
    } else {
      return updateControl;
    }
  }

  /**
   * Converts a ReconcileProgress to a DeleteControl.
   *
   * @param resource The resource being reconciled.
   * @param reconcileProgress The ReconcileProgress object.
   * @param <S> The type of the status.
   * @param <T> The type of the spec.
   * @param <O> The type of the resource, extending BaseResource.
   * @return A DeleteControl object.
   */
  public static <S, T, O extends BaseResource<?, ?, ?, ?, ?>> DeleteControl toDeleteControl(
      O resource, ReconcileProgress reconcileProgress) {
    if (reconcileProgress.isRequeue()) {
      return DeleteControl.noFinalizerRemoval()
          .rescheduleAfter(reconcileProgress.getRequeueAfterDuration());
    } else {
      return DeleteControl.defaultDelete();
    }
  }

  /**
   * Gets or creates a secondary Kubernetes resource. The initial lookup reports the resource as
   * missing only when the API server said so or when the failure is one the create loop retries
   * anyway, so that a read the API server refused is not taken for a missing resource and created
   * again. The lookups of the retry loop stay lenient, since a failed read there only means that
   * the create has to be retried.
   *
   * @param client The KubernetesClient.
   * @param resource The desired resource to get or create.
   * @param <T> The type of the resource, extending HasMetadata.
   * @return An Optional containing the created or existing resource.
   * @throws KubernetesClientException if the API server refused the read, or if the resource
   *     could not be created.
   */
  public static <T extends HasMetadata> Optional<T> getOrCreateSecondaryResource(
      final KubernetesClient client, final T resource) {
    Optional<T> current = getResourceStrictly(client, resource);
    if (current.isEmpty()) {
      // Adding retry logic to overcome known k8s issue:
      // https://github.com/kubernetes/kubernetes/issues/67761
      long maxAttempts = API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS.getValue();
      long attemptCount = 1;
      while (true) {
        try {
          current = Optional.ofNullable(client.resource(resource).create());
          break;
        } catch (KubernetesClientException e) {
          if (log.isErrorEnabled()) {
            log.error(
                "Failed to request resource with responseCode={} attemptCount={}/{}",
                e.getCode(),
                attemptCount,
                maxAttempts);
          }
          if (e.getCode() == HTTP_CONFLICT) {
            if (isConflictForExistingResource(e)) {
              current = getResource(client, resource);
              if (current.isPresent()) {
                return current;
              }
            }
          } else if (e.getCode() == Constants.HTTP_TOO_MANY_REQUESTS) {
            log.debug("Server returned 429 Too Many Requests, will retry with backoff");
          } else if (isTransientError(e) || e.getCode() == HTTP_INTERNAL_ERROR) {
            // GET to avoid a duplicate create for transport failures, timeouts and transient 5xx
            current = getResource(client, resource);
            if (current.isPresent()) {
              return current;
            }
          } else {
            throw e;
          }
          if (++attemptCount > maxAttempts) {
            log.error("Max Retries exceeded while trying to create resource");
            throw e;
          }
          if (shouldBackoffBeforeRetry(e)) {
            BackoffUtils.backoffSleep(e, attemptCount, maxAttempts);
          }
        }
      }
    }
    return current;
  }

  /**
   * Adds an owner reference to a list of secondary resources, linking them to a primary owner.
   *
   * @param client The KubernetesClient.
   * @param resources The List of HasMetadata resources to modify.
   * @param owner The primary owner resource.
   */
  public static void addOwnerReferenceSecondaryResource(
      final KubernetesClient client, final List<HasMetadata> resources, final HasMetadata owner) {

    resources.forEach(
        r -> {
          ObjectMeta metaData =
              new ObjectMetaBuilder(r.getMetadata())
                  .addToOwnerReferences(buildOwnerReferenceTo(owner))
                  .build();
          r.setMetadata(metaData);
        });
    client.resourceList(resources).forceConflicts().serverSideApply();
  }

  /**
   * Retrieves a Kubernetes resource by its desired state for the create path, reporting a resource
   * that could not be read as absent. The create loop re-reads on an AlreadyExists conflict and
   * resolves the actual state anyway, so a failed read there only means that the create has to be
   * retried. A caller which decides from the answer must not read this way, since it cannot tell
   * a missing resource from one it failed to read.
   *
   * @param client The KubernetesClient.
   * @param desired The desired state of the resource.
   * @param <T> The type of the resource, extending HasMetadata.
   * @return An Optional containing the retrieved resource, or empty if not found or not readable.
   */
  static <T extends HasMetadata> Optional<T> getResource(
      final KubernetesClient client, final T desired) {
    try {
      return getResourceStrictly(client, desired);
    } catch (KubernetesClientException e) {
      log.warn(
          "The API server refused to read the resource with responseCode={}, considering it"
              + " absent.",
          e.getCode(),
          e);
      return Optional.empty();
    }
  }

  /**
   * Retrieves a Kubernetes resource by its desired state, telling a missing resource apart from a
   * read the API server refused. A failure the create path retries anyway, such as a transient one
   * or a throttled request, keeps reporting the resource as absent, since that path re-reads on an
   * AlreadyExists conflict and still resolves the actual state.
   *
   * @param client The KubernetesClient.
   * @param desired The desired state of the resource.
   * @param <T> The type of the resource, extending HasMetadata.
   * @return An Optional containing the retrieved resource, or empty if not found or not reachable.
   * @throws KubernetesClientException if the API server refused the read.
   */
  private static <T extends HasMetadata> Optional<T> getResourceStrictly(
      final KubernetesClient client, final T desired) {
    try {
      return Optional.ofNullable(client.resource(desired).get());
    } catch (KubernetesClientException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        return Optional.empty();
      }
      if (isTransientError(e)
          || e.getCode() == HTTP_INTERNAL_ERROR
          || e.getCode() == Constants.HTTP_TOO_MANY_REQUESTS) {
        log.warn(
            "Failed to read the resource with responseCode={}, considering it absent.",
            e.getCode(),
            e);
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * Deletes a Kubernetes resource if it exists.
   *
   * @param client The KubernetesClient.
   * @param resource The resource to delete.
   * @param forceDelete If true, force deletes the resource with a grace period of 0.
   * @param <T> The type of the resource, extending HasMetadata.
   */
  public static <T extends HasMetadata> void deleteResourceIfExists(
      final KubernetesClient client, final T resource, boolean forceDelete) {
    try {
      if (forceDelete) {
        client.resource(resource).withGracePeriod(0L).delete();
      } else {
        client
            .resource(resource)
            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
            .withTimeout(RECONCILER_FOREGROUND_REQUEST_TIMEOUT_SECONDS.getValue(), TimeUnit.SECONDS)
            .delete();
      }
    } catch (KubernetesClientException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        log.info("Pod to delete does not exist, proceeding...");
      } else {
        throw e;
      }
    }
  }

  private static boolean shouldBackoffBeforeRetry(KubernetesClientException e) {
    if (BackoffUtils.getRetryAfterMillis(e) != null) {
      return true;
    }
    return switch (e.getCode()) {
      // A broken connection needs the delay most, and never carries a Retry-After to ask for one.
      case HTTP_CONFLICT, Constants.HTTP_TOO_MANY_REQUESTS, NO_RESPONSE_CODE -> true;
      default -> false;
    };
  }

  /**
   * Whether the given reconciliation is the first attempt at the resource rather than a retry.
   * {@link RetryInfo#getAttemptCount()} is 0 for an execution that is not a retry, and the info is
   * absent altogether when retries are not configured.
   *
   * @param context The reconciliation context.
   * @return True if this execution is not a retry, false otherwise.
   */
  public static boolean isFirstAttempt(Context<?> context) {
    return context.getRetryInfo().map(RetryInfo::getAttemptCount).orElse(0) == 0;
  }

  /**
   * Whether the given failure is expected to clear without anyone acting on it, so that a caller
   * may wait it out rather than report it. A broken connection and an overloaded or proxied server
   * qualify; a rejection by a reachable API server and a client side rejection do not.
   *
   * @param e The failure to classify.
   * @return True if the failure is expected to clear on its own, false otherwise.
   */
  public static boolean isTransientError(KubernetesClientException e) {
    return switch (e.getCode()) {
      case NO_RESPONSE_CODE -> brokeOnTheWayToTheApiServer(e);
      case HTTP_CLIENT_TIMEOUT, HTTP_BAD_GATEWAY, HTTP_UNAVAILABLE, HTTP_GATEWAY_TIMEOUT -> true;
      default -> false;
    };
  }

  /**
   * Whether the given failure left the request unanswered, so that asking again may yet work. It
   * is defined as the complement of the status-less failures that repeating cannot change: a
   * rejection the client raised before sending anything, which carries no cause at all; an answer
   * that arrived but could not be parsed; a certificate the peer could not prove; and the caller
   * being interrupted, which is a decision to stop rather than a failure. Anything else that
   * carries a cause counts as unanswered.
   *
   * <p>Naming what cannot work, rather than what can, keeps this from tracking the exception types
   * of whichever HTTP client is plugged in. A connection the peer closes mid-response is the case
   * that matters: the client in use reports it with a type of its own which is not even an {@link
   * java.io.IOException}, so any list of recognized connection failures would silently miss it.
   *
   * @param e The failure to inspect.
   * @return True if the request went unanswered, false otherwise.
   */
  private static boolean brokeOnTheWayToTheApiServer(KubernetesClientException e) {
    // Bounded walk: a cyclic cause chain must not hang the reconciler. The marker is nested, since
    // fabric8 wraps the failure and its own HTTP client wraps it again.
    Throwable cause = e.getCause();
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      // The trust failures are the ones fabric8 itself refuses to retry. Keying on them rather
      // than on the handshake that reported them keeps a handshake that merely timed out
      // retriable. An interruption is matched on InterruptedException rather than on
      // InterruptedIOException, which a plain read timeout also extends.
      if (cause instanceof JsonProcessingException
          || cause instanceof CertificateException
          || cause instanceof CertPathValidatorException
          || cause instanceof CertPathBuilderException
          || cause instanceof SSLPeerUnverifiedException
          || cause instanceof InterruptedException) {
        return false;
      }
      cause = cause.getCause();
    }
    return e.getCause() != null;
  }

  /**
   * Clones an object using JSON serialization and deserialization.
   *
   * @param object The object to clone.
   * @param <T> The type of the object.
   * @return A deep copy of the object.
   */
  public static <T> T clone(T object) {
    if (object == null) {
      return null;
    }
    try {
      ObjectMapper mapper = ModelUtils.objectMapper;
      return (T) mapper.readValue(mapper.writeValueAsString(object), object.getClass());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
