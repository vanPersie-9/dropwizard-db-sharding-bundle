package io.appform.dropwizard.sharding.query;

import lombok.experimental.UtilityClass;
import org.hibernate.Session;
import org.hibernate.query.Query;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

@UtilityClass
public class QueryUtils {


    public <T> Query<T> createQuery(final Session session,
                                    final Class<T> entityClass,
                                    final QuerySpec<T, T> querySpec) {
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<T> criteria = builder.createQuery(entityClass);
        Root<T> root = criteria.from(entityClass);
        querySpec.apply(root, criteria, builder);
        return session.createQuery(criteria);
    }
}
