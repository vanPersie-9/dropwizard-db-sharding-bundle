package io.appform.dropwizard.sharding.listeners;

public interface TransactionListener {

    void beforeExecute(final ListenerContext listenerContext);

    void afterExecute(final ListenerContext listenerContext);

    void afterException(final ListenerContext listenerContext, final Throwable e);
}
