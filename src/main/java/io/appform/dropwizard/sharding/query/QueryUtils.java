package io.appform.dropwizard.sharding.query;

import lombok.experimental.UtilityClass;
import org.hibernate.Session;
import org.hibernate.query.Query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import java.util.Collection;

@UtilityClass
public class QueryUtils {

    public static <T> Predicate equalityFilter(final CriteriaBuilder criteriaBuilder,
                                               final Root<T> queryRoot,
                                               final String column,
                                               final Object value) {
        return criteriaBuilder.equal(queryRoot.get(column), value);
    }

    public static <T> Predicate notEqualFilter(final CriteriaBuilder criteriaBuilder,
                                               final Root<T> queryRoot,
                                               final String column,
                                               final Object value) {
        return criteriaBuilder.notEqual(queryRoot.get(column), value);
    }

    public static <T> Predicate inFilter(final Root<T> queryRoot,
                                         final String column,
                                         final Collection<String> values) {
        return queryRoot.get(column).in(values);
    }
}
