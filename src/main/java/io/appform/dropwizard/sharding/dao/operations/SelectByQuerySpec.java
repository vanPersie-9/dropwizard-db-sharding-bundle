package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.query.QuerySpec;
import java.util.List;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

@Data
@SuperBuilder
public class SelectByQuerySpec<T, R> extends OpContext<R> {

  private QuerySpec<T, T> querySpec;

  @Builder.Default
  private Function<List<T>, R> afterSelect = t -> (R) t;

  @NonNull
  private Function<QuerySpec, List<T>> getter; //TODO:V rename to selector

  @Override
  public R apply(Session session) {
    return afterSelect.apply(getter.apply(querySpec));
  }

  @Override
  public OpType getOpType() {
    return OpType.SELECT_BY_QUERY_SPEC;
  }

  @Override
  public <R1> R1 visit(OpContextVisitor<R1> visitor) {
    return visitor.visit(this);
  }
}
