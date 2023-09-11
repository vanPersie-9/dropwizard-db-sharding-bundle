package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.appform.dropwizard.sharding.dao.RelationalDao;
import io.appform.dropwizard.sharding.dao.testdata.entities.RelationalEntity;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TransactionMetricObserverTest {

    private TransactionMetricObserver transactionMetricObserver;
    private TransactionMetricManager metricManager;

    @Before
    public void setup() {
        this.metricManager = Mockito.mock(TransactionMetricManager.class);
        this.transactionMetricObserver = new TransactionMetricObserver(metricManager);
    }

    @Test
    public void testExecuteWhenMetricNotApplicable() {
        Mockito.doReturn(false).when(metricManager).isMetricApplicable(null);
        Assert.assertEquals(terminate(),
                transactionMetricObserver.execute(TransactionExecutionContext.builder().build(), this::terminate));
    }

    @Test
    public void testExecuteWithNoException() {
        val context = TransactionExecutionContext.builder()
                .entityClass(RelationalDao.class)
                .shardName("shard")
                .daoClass(RelationalEntity.class)
                .build();
        val entityMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();
        val shardMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();
        val daoMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();

        Mockito.doReturn(entityMetricData).when(metricManager).getEntityMetricData(context.getEntityClass());
        Mockito.doReturn(shardMetricData).when(metricManager).getShardMetricData(context.getShardName());

        Mockito.doReturn("test").when(metricManager).getDaoMetricPrefix(context.getDaoClass());
        Mockito.doReturn(daoMetricData).when(metricManager).getDaoOpMetricData("test", context);

        Mockito.doReturn(true).when(metricManager).isMetricApplicable(context.getEntityClass());

        Assert.assertEquals(terminate(), transactionMetricObserver.execute(context, this::terminate));
        validateCache(entityMetricData, shardMetricData, daoMetricData, "test", context);
        validateMetrics(entityMetricData, shardMetricData, daoMetricData, 1, 0);
    }

    @Test
    public void testExecuteWithException() {
        val context = TransactionExecutionContext.builder()
                .entityClass(RelationalDao.class)
                .shardName("shard")
                .daoClass(RelationalEntity.class)
                .build();
        val entityMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();
        val shardMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();
        val daoMetricData = MetricData.builder()
                .timer(new Timer())
                .total(new Meter())
                .success(new Meter())
                .failed(new Meter())
                .build();

        Mockito.doReturn(entityMetricData).when(metricManager).getEntityMetricData(context.getEntityClass());
        Mockito.doReturn(shardMetricData).when(metricManager).getShardMetricData(context.getShardName());

        Mockito.doReturn("test").when(metricManager).getDaoMetricPrefix(context.getDaoClass());
        Mockito.doReturn(daoMetricData).when(metricManager).getDaoOpMetricData("test", context);

        Mockito.doReturn(true).when(metricManager).isMetricApplicable(context.getEntityClass());

        Assert.assertThrows(RuntimeException.class, () -> transactionMetricObserver.execute(context, this::terminateWithException));
        validateCache(entityMetricData, shardMetricData, daoMetricData, "test", context);
        validateMetrics(entityMetricData, shardMetricData, daoMetricData, 0, 1);
    }

    private void validateCache(final MetricData entityMetricData,
                               final MetricData shardMetricData,
                               final MetricData daoMetricData,
                               final String daoMetricPrefix,
                               final TransactionExecutionContext context) {
        Assert.assertEquals(1, transactionMetricObserver.getEntityMetricCache().size());
        Assert.assertEquals(entityMetricData, transactionMetricObserver.getEntityMetricCache().get(context.getEntityClass()));

        Assert.assertEquals(1, transactionMetricObserver.getShardMetricCache().size());
        Assert.assertEquals(shardMetricData, transactionMetricObserver.getShardMetricCache().get(context.getShardName()));

        Assert.assertEquals(1, transactionMetricObserver.getDaoMetricPrefixCache().size());
        Assert.assertEquals(daoMetricPrefix,
                transactionMetricObserver.getDaoMetricPrefixCache().get(context.getDaoClass()));

        Assert.assertEquals(1, transactionMetricObserver.getDaoToOpTypeMetricCache().size());
        Assert.assertEquals(daoMetricData,
                transactionMetricObserver.getDaoToOpTypeMetricCache().get(daoMetricPrefix).get(context.getOpType()));
    }

    private void validateMetrics(final MetricData entityMetricData,
                                 final MetricData shardMetricData,
                                 final MetricData daoMetricData,
                                 final int successCount,
                                 final int failedCount) {
        Assert.assertEquals(1, entityMetricData.getTotal().getCount());
        Assert.assertEquals(1, shardMetricData.getTotal().getCount());
        Assert.assertEquals(1, daoMetricData.getTotal().getCount());

        Assert.assertEquals(1, entityMetricData.getTimer().getCount());
        Assert.assertEquals(1, shardMetricData.getTimer().getCount());
        Assert.assertEquals(1, daoMetricData.getTimer().getCount());

        Assert.assertEquals(successCount, entityMetricData.getSuccess().getCount());
        Assert.assertEquals(successCount, shardMetricData.getSuccess().getCount());
        Assert.assertEquals(successCount, daoMetricData.getSuccess().getCount());

        Assert.assertEquals(failedCount, entityMetricData.getFailed().getCount());
        Assert.assertEquals(failedCount, shardMetricData.getFailed().getCount());
        Assert.assertEquals(failedCount, daoMetricData.getFailed().getCount());
    }

    private Integer terminate() {
         return 1;
    }

    private Integer terminateWithException() {
        throw new RuntimeException();
    }
}
