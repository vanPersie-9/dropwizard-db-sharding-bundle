package io.appform.dropwizard.sharding.dao.interceptors;

import lombok.experimental.UtilityClass;
import org.junit.Assert;
import org.slf4j.MDC;

@UtilityClass
public class InterceptorTestUtil {

    private static final String ENTITY_START = "entity.start";
    private static final String ENTITY_END = "entity.end";

    private static final String DAO_START = "dao.start";

    private static final String DAO_END = "dao.end";

    public void validateThreadLocal(final Class<?> daoClass,
                                    final Class<?> entityClass) {
        Assert.assertEquals(daoClass.getSimpleName(), MDC.get(DAO_START));
        Assert.assertEquals(daoClass.getSimpleName(), MDC.get(DAO_END));
        Assert.assertEquals(entityClass.getSimpleName(), MDC.get(ENTITY_START));
        Assert.assertEquals(entityClass.getSimpleName(), MDC.get(ENTITY_END));
    }
}
