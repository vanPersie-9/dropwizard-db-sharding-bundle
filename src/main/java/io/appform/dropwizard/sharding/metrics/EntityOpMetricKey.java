package io.appform.dropwizard.sharding.metrics;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@Builder
@EqualsAndHashCode
public class EntityOpMetricKey {
    Class<?> entityClass;
    Class<?> daoClass;
    String opType;
    String lockedContextMode;
}
