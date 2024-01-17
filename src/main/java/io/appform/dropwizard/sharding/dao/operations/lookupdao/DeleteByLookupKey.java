package io.appform.dropwizard.sharding.dao.operations.lookupdao;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

/**
 * Delete an entity by lookup Key.
 */
@Data
@SuperBuilder
public class DeleteByLookupKey extends OpContext<Boolean> {

  @NonNull
  private Function<String, Boolean> handler;
  @NonNull
  private String id;

  @Override
  public Boolean apply(Session session) {
    return handler.apply(id);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.DELETE_BY_LOOKUP_KEY;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
