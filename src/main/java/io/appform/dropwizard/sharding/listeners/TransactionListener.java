package io.appform.dropwizard.sharding.listeners;

public interface TransactionListener {

    void beforeExecute(final TransactionListenerContext listenerContext);

    void afterExecute(final TransactionListenerContext listenerContext);

    void afterException(final TransactionListenerContext listenerContext, final Throwable e);
}
