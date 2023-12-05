package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.dao.UpdateOperationMeta;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

@Data
@SuperBuilder
public class UpdateByQuery<T> extends OpContext<Integer> {

  @NonNull
  private Function<UpdateOperationMeta, Integer> updater;
  @NonNull
  private UpdateOperationMeta updateOperationMeta;

  @Override
  public Integer apply(Session session) {
    return updater.apply(updateOperationMeta);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.UPDATE_BY_QUERY;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
