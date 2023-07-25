package io.appform.dropwizard.sharding.listeners;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class TransactionListenerContext {
    Class<?> entityClass;
    Class<?> daoClass;
    String shardName;
    String opType;
    String lockedContextMode;
}
