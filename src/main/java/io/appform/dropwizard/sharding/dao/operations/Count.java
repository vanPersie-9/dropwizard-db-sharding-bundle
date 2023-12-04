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
public class Count extends OpContext<Long> {

  private DetachedCriteria criteria;

  private Function<DetachedCriteria, Long> counter;

  @Override
  public Long apply(Session session) {
    return counter.apply(criteria);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.COUNT;
  }
}
