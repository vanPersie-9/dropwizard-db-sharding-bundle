package io.appform.dropwizard.sharding.dao.operations;

import io.appform.dropwizard.sharding.query.QuerySpec;
import lombok.Builder;
import lombok.Getter;
import org.hibernate.criterion.DetachedCriteria;

@Builder
@Getter
public class ScrollParam<T> {

  private DetachedCriteria criteria;
  private QuerySpec<T, T> querySpec;

}