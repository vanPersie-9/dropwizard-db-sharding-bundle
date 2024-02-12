package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.query.QuerySpec;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.hibernate.criterion.DetachedCriteria;

@Builder
@Getter
public class ScrollParam<T> {

  public DetachedCriteria criteria;
  public QuerySpec<T, T> querySpec;

}