package io.appform.dropwizard.sharding.dao.interceptors;

import lombok.experimental.UtilityClass;
import org.junit.jupiter.api.Assertions;
import org.slf4j.MDC;

@UtilityClass
public class InterceptorTestUtil {

    public static final String ENTITY_START = "entity.start";
    public static final String ENTITY_END = "entity.end";

    public static final String DAO_START = "dao.start";

    public static final String DAO_END = "dao.end";

    public void validateThreadLocal(final Class<?> daoClass,
                                    final Class<?> entityClass) {
        Assertions.assertEquals(daoClass.getSimpleName(), MDC.get(DAO_START));
        Assertions.assertEquals(daoClass.getSimpleName(), MDC.get(DAO_END));
        Assertions.assertEquals(entityClass.getSimpleName(), MDC.get(ENTITY_START));
        Assertions.assertEquals(entityClass.getSimpleName(), MDC.get(ENTITY_END));
    }
}
