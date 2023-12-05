package io.appform.dropwizard.sharding.dao.operations.lookupdao;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.val;
import org.hibernate.Session;

@Data
@SuperBuilder
public class CreateOrUpdate<T> extends OpContext<T> {

  @NonNull
  private String id;
  @NonNull
  private UnaryOperator<T> mutator;
  @NonNull
  private Supplier<T> entityGenerator;
  @NonNull
  private Function<String, T> getLockedForWrite;
  @NonNull
  private Function<String, T> getter;
  @NonNull
  private Function<T, T> saver;
  @NonNull
  private Consumer<T> updater;

  @Override
  public T apply(Session session) {
    T result = getLockedForWrite.apply(id);
    if (null == result) {
      val newEntity = entityGenerator.get();
      if (null != newEntity) {
        return saver.apply(newEntity);
      }
      return null;
    }
    val updated = mutator.apply(result);
    if (null != updated) {
      updater.accept(updated);
    }
    return getter.apply(id);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.CREATE_OR_UPDATE;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
