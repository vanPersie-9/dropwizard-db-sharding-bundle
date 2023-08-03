package io.appform.dropwizard.sharding.dao.listeners;

import io.appform.dropwizard.sharding.dao.testdata.entities.OrderItem;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
import io.appform.dropwizard.sharding.listeners.TransactionListenerFactory;

public class OrderItemTestListenerFactory implements TransactionListenerFactory {

    @Override
    public TransactionListener createListener(Class<?> daoClass, Class<?> entityClass, String shardName) {
        if (daoClass == OrderItem.class) {
            return new TestListener(daoClass, entityClass, shardName);
        }
        return null;
    }
}
