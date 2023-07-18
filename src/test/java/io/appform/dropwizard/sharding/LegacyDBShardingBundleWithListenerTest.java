package io.appform.dropwizard.sharding;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import io.appform.dropwizard.sharding.dao.listeners.TestListenerFactory;
import io.appform.dropwizard.sharding.dao.testdata.entities.Order;
import io.appform.dropwizard.sharding.dao.testdata.entities.OrderItem;
import io.appform.dropwizard.sharding.listeners.TransactionListenerFactory;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LegacyDBShardingBundleWithListenerTest extends DBShardingBundleTestBase {

    private final List<TransactionListenerFactory> allEntitiesListenerFactories = Lists.newArrayList(new TestListenerFactory(),
            new TestListenerFactory());
    private final List<TransactionListenerFactory> orderListenerFactories = Lists.newArrayList(new TestListenerFactory());


    @Override
    protected DBShardingBundleBase<DBShardingBundleTestBase.TestConfig> getBundle() {
        return new DBShardingBundle<DBShardingBundleTestBase.TestConfig>(Order.class, OrderItem.class) {
            @Override
            protected ShardedHibernateFactory getConfig(DBShardingBundleTestBase.TestConfig config) {
                return testConfig.getShards();
            }

            @Override
            protected List<TransactionListenerFactory> getTransactionListenerFactories() {
                return allEntitiesListenerFactories;
            }

            @Override
            protected Map<Class<?>, List<TransactionListenerFactory>> getEntityTransactionListenerFactories() {
                return ImmutableMap.of(OrderItem.class, orderListenerFactories);
            }
        };
    }

    @Test
    public void testListeners() {
        val bundle = getBundle();
        val listenerFactories = bundle.getListenerFactories();

        val orderEntityFactories = listenerFactories.get(Order.class);
        Assert.assertEquals(allEntitiesListenerFactories, orderEntityFactories);

        val orderItemEntityFactories = listenerFactories.get(OrderItem.class);

        val combinedOrderItemEntityFactories = new ArrayList<>(allEntitiesListenerFactories);
        combinedOrderItemEntityFactories.addAll(orderListenerFactories);

        Assert.assertEquals(combinedOrderItemEntityFactories.size(), orderItemEntityFactories.size());

        Assert.assertTrue(combinedOrderItemEntityFactories.containsAll(orderItemEntityFactories));
    }

}
