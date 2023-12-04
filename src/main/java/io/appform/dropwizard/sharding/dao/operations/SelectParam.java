package io.appform.dropwizard.sharding.dao.operations;

import lombok.Builder;
import lombok.Data;
import org.hibernate.criterion.DetachedCriteria;

@Builder
@Data
public class SelectParam {
  private DetachedCriteria criteria;
  private int start;
  private int numRows;
}