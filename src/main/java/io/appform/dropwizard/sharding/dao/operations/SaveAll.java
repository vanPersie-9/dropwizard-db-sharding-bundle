package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.Collection;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

@Data
@SuperBuilder
public class SaveAll<T> extends OpContext<Boolean> {

  @NonNull
  private Collection<T> entities;
  @NonNull
  private Function<Collection<T>, Boolean> saver;


  @Override
  public Boolean apply(Session session) {
    return saver.apply(entities);
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.SAVE_ALL;
  }
}
