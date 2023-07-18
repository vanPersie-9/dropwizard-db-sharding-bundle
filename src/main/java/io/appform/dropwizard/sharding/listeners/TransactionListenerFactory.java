package io.appform.dropwizard.sharding.listeners;

public interface TransactionListenerFactory {

    TransactionListener createListener(final Class<?> daoClass,
                                       final Class<?> entityClass,
                                       final String shardName);
}
