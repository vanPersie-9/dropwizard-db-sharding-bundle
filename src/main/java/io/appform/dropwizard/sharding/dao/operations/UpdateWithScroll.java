package io.appform.dropwizard.sharding.dao.operations;

import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;

@Data
@SuperBuilder
public class UpdateWithScroll<T> extends OpContext<Boolean> {

  @NonNull
  private ScrollParam scrollParam;
  @NonNull
  private Function<ScrollParam, ScrollableResults> scroller;
  @Builder.Default
  private Function<T, T> mutator = t -> t;
  private BiConsumer<T, T> updater;
  private BooleanSupplier updateNext;

  @Override
  public Boolean apply(Session session) {
    ScrollableResults scrollableResults = scroller.apply(scrollParam);
    boolean updateNextObject = true;
    try {
      while (scrollableResults.next() && updateNextObject) {
        final T entity = (T) scrollableResults.get(
            0);
        if (null == entity) {
          return false;
        }
        final T newEntity = mutator.apply(
            entity);
        if (null == newEntity) {
          return false;
        }
        updater.accept(
            entity,
            newEntity);
        updateNextObject = updateNext.getAsBoolean();
      }
    } finally {
      scrollableResults.close();
    }
    return true;
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.UPDATE_WITH_SCROLL;
  }
}
