package io.appform.dropwizard.sharding.metrics;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class EntityOpMetricKey {
    Class<?> entityClass;
    Class<?> daoClass;
    String opType;
    String lockedContextMode;
}
