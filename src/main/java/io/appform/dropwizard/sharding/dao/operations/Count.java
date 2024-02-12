package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

/**
 * Returns count of records matching given criteria for a shard.
 */
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
  public OpType getOpType() {
    return OpType.COUNT;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
