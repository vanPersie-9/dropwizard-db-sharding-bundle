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
public class Run<T> extends OpContext<T> {

  @NonNull
  Function<Session, T> handler;

  @Override
  public T apply(Session session) {
    return handler.apply(session);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.RUN;
  }
}
