package io.appform.dropwizard.sharding.dao.interceptors;

import io.appform.dropwizard.sharding.interceptors.TransactionInterceptor;
import io.appform.dropwizard.sharding.interceptors.TransactionInterceptorExecutor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.slf4j.MDC;

@Slf4j
public class DaoClassLocalInterceptor implements TransactionInterceptor {

    @Override
    public <T> T intercept(TransactionInterceptorExecutor<T> transactionInterceptorExecutor) {
        val context = transactionInterceptorExecutor.getContext();
        MDC.put("dao.start", context.getDaoClass().getSimpleName());
        val response = transactionInterceptorExecutor.proceed();
        MDC.put("dao.end", context.getDaoClass().getSimpleName());
        return response;
    }
}
