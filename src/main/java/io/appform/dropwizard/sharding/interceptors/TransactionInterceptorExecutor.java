package io.appform.dropwizard.sharding.interceptors;

import lombok.val;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class TransactionInterceptorExecutor<T> {

    private final Iterator<TransactionInterceptor> iterator;

    private final TransactionExecutionContext transactionExecutionContext;

    private final Supplier<T> supplier;

    public TransactionInterceptorExecutor(final List<TransactionInterceptor> interceptors,
                                          final TransactionExecutionContext transactionExecutionContext,
                                          final Supplier<T> supplier) {
        this.transactionExecutionContext = transactionExecutionContext;
        this.iterator = interceptors.iterator();
        this.supplier = supplier;
    }

    private Optional<TransactionInterceptor> getNextInterceptor() {
        return iterator.hasNext()
                ? Optional.of(iterator.next())
                : Optional.empty();
    }

    public TransactionExecutionContext getContext() {
        return transactionExecutionContext;
    }

    public T proceed() {
        val nextInterceptor = getNextInterceptor().orElse(null);
        if (nextInterceptor == null) {
            return supplier.get();
        }
        return nextInterceptor.intercept(this);
    }
}
