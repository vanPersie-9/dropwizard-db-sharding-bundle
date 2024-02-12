package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.query.QuerySpec;
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
public class CountByQuerySpec extends OpContext<Long> {

  private QuerySpec querySpec;

  private Function<QuerySpec, Long> counter;

  @Override
  public Long apply(Session session) {
    return counter.apply(querySpec);
  }

  @Override
  public OpType getOpType() {
    return OpType.COUNT_BY_QUERY_SPEC;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
