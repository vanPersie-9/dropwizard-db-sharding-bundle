package io.appform.dropwizard.sharding.dao.interceptors;

import io.appform.dropwizard.sharding.interceptors.TransactionInterceptor;
import io.appform.dropwizard.sharding.interceptors.TransactionInterceptorExecutor;
import lombok.val;
import org.slf4j.MDC;

public class EntityClassThreadLocalInterceptor implements TransactionInterceptor {

    @Override
    public <T> T intercept(TransactionInterceptorExecutor<T> transactionInterceptorExecutor) {
        val context = transactionInterceptorExecutor.getContext();
        MDC.put("entity.start", context.getEntityClass().getSimpleName());
        val response = transactionInterceptorExecutor.proceed();
        MDC.put("entity.end", context.getEntityClass().getSimpleName());
        return response;
    }
}
