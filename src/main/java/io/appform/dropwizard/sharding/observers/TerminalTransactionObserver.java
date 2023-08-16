package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;

import java.util.function.Supplier;

/**
 *
 */
public class TerminalTransactionObserver extends TransactionObserver {
    public TerminalTransactionObserver() {
        super(null);
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        return proceed(context, supplier);
    }
}
