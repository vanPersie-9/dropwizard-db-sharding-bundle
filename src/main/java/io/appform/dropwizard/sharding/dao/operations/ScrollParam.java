package io.appform.dropwizard.sharding.dao.operations;

import lombok.Builder;
import lombok.Getter;
import org.hibernate.criterion.DetachedCriteria;

@Builder
public class ScrollParam {

  @Getter
  private DetachedCriteria criteria;
}