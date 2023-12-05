package io.appform.dropwizard.sharding.execution;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class TransactionExecutionContext {
    String commandName; // method or command name that gets executed across shards. Ex. ScatterGather.
    String shardName;
    Class<?> daoClass;
    Class<?> entityClass;

    @NonNull
    OpContext<?> opContext;

}
