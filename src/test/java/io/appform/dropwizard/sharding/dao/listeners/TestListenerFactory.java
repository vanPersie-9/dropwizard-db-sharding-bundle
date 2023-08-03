package io.appform.dropwizard.sharding.dao.listeners;

import io.appform.dropwizard.sharding.listeners.TransactionListener;
import io.appform.dropwizard.sharding.listeners.TransactionListenerFactory;

public class TestListenerFactory implements TransactionListenerFactory {

    @Override
    public TransactionListener createListener(Class<?> daoClass, Class<?> entityClass, String shardName) {
        return new TestListener(daoClass, entityClass, shardName);
    }
}
