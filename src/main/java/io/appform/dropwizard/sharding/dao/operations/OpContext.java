package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

/**
 * Operation to be executed as a transaction on a single shard.
 * @param <T> return type of the operation.
 */
@Data
@SuperBuilder
public abstract class OpContext<T> implements Function<Session, T> {

  @NonNull
  public abstract OpType getOpType();

}
