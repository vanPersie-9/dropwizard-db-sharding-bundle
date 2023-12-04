package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;

@Data
@SuperBuilder
public class GetAndUpdate<T> extends OpContext<Boolean> {

  @NonNull
  private DetachedCriteria criteria;
  @NonNull
  private Function<DetachedCriteria, T> getter;
  @Builder.Default
  private Function<T, T> mutator = t->t;
  private BiConsumer<T, T> updater;

  @Override
  public Boolean apply(Session session) {
    T entity = getter.apply(criteria);
    if (null == entity) {
      return false;
    }
    T newEntity = mutator.apply(entity);
    if (null == newEntity) {
      return false;
    }
    updater.accept(entity, newEntity);
    return true;
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.GET_AND_UPDATE;
  }
}
