package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

@Data
@SuperBuilder
public class RunWithCriteria<T> extends OpContext<T> {

  @NonNull
  private Function<DetachedCriteria, T> handler;
  @NonNull
  private DetachedCriteria detachedCriteria;

  @Override
  public T apply(Session session) {
    return handler.apply(detachedCriteria);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.RUN;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
