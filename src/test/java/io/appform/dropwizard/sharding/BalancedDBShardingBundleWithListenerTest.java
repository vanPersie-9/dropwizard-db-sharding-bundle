package io.appform.dropwizard.sharding;

import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import io.appform.dropwizard.sharding.dao.listeners.OrderItemTestListenerFactory;
import io.appform.dropwizard.sharding.dao.listeners.TestListenerFactory;
import io.appform.dropwizard.sharding.dao.testdata.entities.Order;
import io.appform.dropwizard.sharding.dao.testdata.entities.OrderItem;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class BalancedDBShardingBundleWithListenerTest extends DBShardingBundleTestBase {

    @Override
    protected DBShardingBundleBase<TestConfig> getBundle() {
        return new BalancedDBShardingBundle<TestConfig>(Order.class, OrderItem.class) {
            @Override
            protected ShardedHibernateFactory getConfig(TestConfig config) {
                return testConfig.getShards();
            }
        };
    }

    @Test
    public void testListeners() {
        val bundle = getBundle();
        val inputListeners = Lists.newArrayList(new TestListenerFactory(), new TestListenerFactory(),
                new OrderItemTestListenerFactory());
        bundle.registerTransactionListenerFactories(inputListeners);
        val listenerFactories = bundle.getListenerFactories();
        Assert.assertEquals(3, listenerFactories.size());
        Assert.assertTrue(listenerFactories.containsAll(inputListeners));
    }

}
