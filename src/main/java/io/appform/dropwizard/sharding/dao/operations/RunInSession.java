package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

/**
 * Run any action within a session on specific shard.
 *
 * @param <T> return type of the given action.
 */
@Data
@SuperBuilder
public class RunInSession<T> extends OpContext<T> {

  @NonNull
  Function<Session, T> handler;

  @Override
  public T apply(Session session) {
    return handler.apply(session);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.RUN_IN_SESSION;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
