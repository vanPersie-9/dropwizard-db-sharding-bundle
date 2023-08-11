package io.appform.dropwizard.sharding.interceptors;

public interface TransactionInterceptor {
    <T> T intercept(final TransactionInterceptorExecutor<T> transactionInterceptorExecutor);
}
