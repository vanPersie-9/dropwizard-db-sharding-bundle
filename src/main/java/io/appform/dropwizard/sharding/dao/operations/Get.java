package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

/**
 *
 * @param <T> Type of Dao entity
 * @param <R>
 */
@Data
@SuperBuilder
public class Get<T, R> extends OpContext<R> {

  @NonNull
  private DetachedCriteria criteria;
  @NonNull
  private Function<DetachedCriteria, T> getter;
  @Builder.Default
  private Function<T, R> afterGet = t -> (R) t;


  @Override
  public R apply(Session session) {
    return afterGet.apply(getter.apply(criteria));
  }

  @Override
  public OpType getOpType() {
    return OpType.GET;
  }

  @Override
  public <R1> R1 visit(OpContextVisitor<R1> visitor) {
    return visitor.visit(this);
  }
}
