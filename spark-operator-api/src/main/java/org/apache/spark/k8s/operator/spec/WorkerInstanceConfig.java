package org.apache.spark.k8s.operator.spec;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.generator.annotation.Required;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkerInstanceConfig {
  @Required @Builder.Default protected int initWorkers = 0;
  @Required @Builder.Default protected int minWorkers = 0;
  @Required @Builder.Default protected int maxWorkers = 0;
}
