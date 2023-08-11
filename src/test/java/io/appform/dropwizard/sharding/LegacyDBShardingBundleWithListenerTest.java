package io.appform.dropwizard.sharding;

import io.appform.dropwizard.sharding.config.ShardedHibernateFactory;
import io.appform.dropwizard.sharding.dao.testdata.entities.Order;
import io.appform.dropwizard.sharding.dao.testdata.entities.OrderItem;

public class LegacyDBShardingBundleWithListenerTest extends DBShardingBundleTestBase {


    @Override
    protected DBShardingBundleBase<DBShardingBundleTestBase.TestConfig> getBundle() {
        return new DBShardingBundle<DBShardingBundleTestBase.TestConfig>(Order.class, OrderItem.class) {
            @Override
            protected ShardedHibernateFactory getConfig(DBShardingBundleTestBase.TestConfig config) {
                return testConfig.getShards();
            }
        };


    }

//    @Test
//    public void testListeners() {
//        val bundle = getBundle();
//        val inputListeners = Lists.newArrayList(new TestListenerFactory(), new TestListenerFactory(),
//                new OrderItemTestListenerFactory());
//        bundle.registerTransactionListenerFactories(inputListeners);
//        val listenerFactories = bundle.getListenerFactories();
//        Assert.assertEquals(3, listenerFactories.size());
//        Assert.assertTrue(listenerFactories.containsAll(inputListeners));
//    }

}
