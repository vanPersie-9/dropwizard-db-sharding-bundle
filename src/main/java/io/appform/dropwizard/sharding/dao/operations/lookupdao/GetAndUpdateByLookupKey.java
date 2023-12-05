package io.appform.dropwizard.sharding.dao.operations.lookupdao;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

@Data
@SuperBuilder
public class GetAndUpdateByLookupKey<T> extends OpContext<Boolean> {

  private String id;
  private Function<String, T> getter;
  private Function<Optional<T>, T> mutator;
  private Consumer<T> updater;

  @Override
  public Boolean apply(Session session) {
    T entity = getter.apply(id);
    T newEntity = mutator.apply(Optional.ofNullable(entity));
    if (null == newEntity) {
      return false;
    }
    updater.accept(entity);
    return true;
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.GET_AND_UPDATE;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
