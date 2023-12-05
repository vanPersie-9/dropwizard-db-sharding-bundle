package io.appform.dropwizard.sharding.dao.operations;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

@Data
@SuperBuilder
public class Update<T> extends OpContext<Boolean> {

  private DetachedCriteria criteria;
  private int start;
  private int numRows;

  @Builder.Default
  private Function<T, T> mutator = t -> t;

  private Function<SelectParam, List<T>> getter;


  private BiConsumer<T, T> updater;

  @Override
  public Boolean apply(Session session) {
    SelectParam selectParam = SelectParam.<T>builder()
        .criteria(criteria)
        .start(start)
        .numRows(numRows)
        .build();
    List<T> entityList = getter.apply(selectParam);
    if (entityList == null || entityList.isEmpty()) {
      return false;
    }
    for (T oldEntity :
        entityList) {
      if (null == oldEntity) {
        return false;
      }
      T newEntity =
          mutator.apply(
              oldEntity);
      if (null == newEntity) {
        return false;
      }
      updater.accept(oldEntity,
          newEntity);
    }
    return true;
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.UPDATE;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
