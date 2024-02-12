package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.query.QuerySpec;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.hibernate.criterion.DetachedCriteria;

@Builder
@Getter
public class SelectParam<T> {

  public DetachedCriteria criteria;
  public QuerySpec<T, T> querySpec;
  public int start;
  public int numRows;
}