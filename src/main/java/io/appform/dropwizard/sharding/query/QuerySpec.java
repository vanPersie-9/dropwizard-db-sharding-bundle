package io.appform.dropwizard.sharding.query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public interface QuerySpec<T, U> {

    void apply(Root<T> queryRoot, CriteriaQuery<U> query, CriteriaBuilder criteriaBuilder);
}
