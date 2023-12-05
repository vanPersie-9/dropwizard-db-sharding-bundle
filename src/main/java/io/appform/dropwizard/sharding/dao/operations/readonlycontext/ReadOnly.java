package io.appform.dropwizard.sharding.dao.operations.readonlycontext;

import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.OpType;
import java.util.List;
import java.util.function.Function;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.hibernate.Session;

@Data
@SuperBuilder
public class ReadOnly<T> extends OpContext<T> {

  private final String key;
  private final List<Function<T, Void>> operations = Lists.newArrayList();
  private final Function<String, T> getter;


  @Override
  public T apply(Session session) {
    T result = getter.apply(key);
    if (null != result) {
      operations.forEach(operation -> operation.apply(result));
    }
    return result;
  }

  @Override
  public @NonNull OpType getOpType() {
    return OpType.READ_ONLY;
  }

  @Override
  public <R> R visit(OpContextVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
