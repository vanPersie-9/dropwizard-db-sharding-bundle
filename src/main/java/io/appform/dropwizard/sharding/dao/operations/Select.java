package io.appform.dropwizard.sharding.dao.operations;

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
public class Select<T, R> extends OpContext<R> {

  private DetachedCriteria criteria;
  @Builder.Default
  private int start = -1;
  @Builder.Default
  private int numRows = -1;

  @Builder.Default
  private Function<List<T>, R> afterSelect = t -> (R) t;

  @NonNull
  private Function<SelectParam, List<T>> getter;

  @Override
  public R apply(Session session) {
    SelectParam selectParam = SelectParam.<T>builder()
        .criteria(criteria)
        .start(start)
        .numRows(numRows)
        .build();
    return afterSelect.apply(getter.apply(selectParam));
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.SELECT;
  }

  @Override
  public <R1> R1 visit(OpContextVisitor<R1> visitor) {
    return visitor.visit(this);
  }
}
