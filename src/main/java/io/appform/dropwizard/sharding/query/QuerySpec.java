/**
 * The `QuerySpec` interface defines a functional contract for applying query specifications
 * to a JPA (Java Persistence API) Criteria API query.
 *
 * <p>Implementations of this interface can define custom query specifications and apply them
 * to a JPA Criteria query, typically used for building database queries dynamically.
 *
 * @param <T> The type of the entity for which the query specification is applied.
 * @param <U> The type of the result expected from the query.
 *
 * @see javax.persistence.criteria.CriteriaBuilder
 * @see javax.persistence.criteria.CriteriaQuery
 * @see javax.persistence.criteria.Root
 *
 * @author Your Name
 * @version 1.0
 * @since 2023-09-30
 */

package io.appform.dropwizard.sharding.query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

@FunctionalInterface
public interface QuerySpec<T, U> {

    /**
     * Applies the query specification to a JPA Criteria query
     *
     * @param queryRoot       The root entity in the query.
     * @param query           The criteria query to which the specification is applied.
     * @param criteriaBuilder The criteria builder for building query predicates.
     */

    void apply(Root<T> queryRoot, CriteriaQuery<U> query, CriteriaBuilder criteriaBuilder);
}
