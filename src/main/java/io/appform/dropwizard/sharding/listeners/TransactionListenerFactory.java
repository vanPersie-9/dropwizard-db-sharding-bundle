package io.appform.dropwizard.sharding.listeners;

/**
 * Factory for creating instances of transaction listeners.
 * Listener instances can be same or different for the combination of parameters in createListener()
 */
public interface TransactionListenerFactory {

    TransactionListener createListener(final Class<?> daoClass,
                                       final Class<?> entityClass,
                                       final String shardName);
}
