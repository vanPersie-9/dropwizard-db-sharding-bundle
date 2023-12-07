package io.appform.dropwizard.sharding.dao.operations.relationaldao;

import com.google.common.base.Preconditions;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import io.appform.dropwizard.sharding.dao.operations.SelectParam;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

@Data
@SuperBuilder
public class CreateOrUpdateInLockedContext<T, U> extends OpContext<Boolean> {

  @NonNull
  private U lockedEntity;
  @NonNull
  private SelectParam selectParam;
  @NonNull
  private UnaryOperator<T> mutator;
  @NonNull
  private Function<U, T> entityGenerator;
  @NonNull
  private Function<SelectParam, List<T>> selector;
  @NonNull
  private Function<T, T> saver;
  @NonNull
  private BiConsumer<T, T> updater;

  @Override
  public Boolean apply(Session session) {
    List<T> entityList = selector.apply(selectParam);
    if (entityList == null || entityList.isEmpty()) {
      Preconditions.checkNotNull(entityGenerator, "Entity generator " + "can't be " + "null");
      final T newEntity = entityGenerator.apply(lockedEntity);
      Preconditions.checkNotNull(newEntity, "Generated entity " + "can't be " + "null");
      saver.apply(newEntity);
      return true;
    }

    final T oldEntity = entityList.get(0);
    if (null == oldEntity) {
      return false;
    }
    final T newEntity = mutator.apply(oldEntity);
    if (null == newEntity) {
      return false;
    }
    updater.accept(oldEntity, newEntity);
    return true;
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
