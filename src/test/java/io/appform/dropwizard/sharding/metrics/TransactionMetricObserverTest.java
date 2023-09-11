package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.appform.dropwizard.sharding.dao.RelationalDao;
import io.appform.dropwizard.sharding.dao.testdata.entities.RelationalEntity;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TransactionMetricObserverTest {

    private TransactionMetricObserver transactionMetricObserver;
    private TransactionMetricManager metricManager;

    @BeforeEach
    public void setup() {
        this.metricManager = Mockito.mock(TransactionMetricManager.class);
        this.transactionMetricObserver = new TransactionMetricObserver(metricManager);
    }

    @Test
    public void testExecuteWhenMetricNotApplicable() {
        Mockito.doReturn(false).when(metricManager).isMetricApplicable(null);
        Assertions.assertEquals(terminate(),
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

        Assertions.assertEquals(terminate(), transactionMetricObserver.execute(context, this::terminate));
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

        Assertions.assertThrows(RuntimeException.class, () -> transactionMetricObserver.execute(context, this::terminateWithException));
        validateCache(entityMetricData, shardMetricData, daoMetricData, "test", context);
        validateMetrics(entityMetricData, shardMetricData, daoMetricData, 0, 1);
    }

    private void validateCache(final MetricData entityMetricData,
                               final MetricData shardMetricData,
                               final MetricData daoMetricData,
                               final String daoMetricPrefix,
                               final TransactionExecutionContext context) {
        Assertions.assertEquals(1, transactionMetricObserver.getEntityMetricCache().size());
        Assertions.assertEquals(entityMetricData, transactionMetricObserver.getEntityMetricCache().get(context.getEntityClass()));

        Assertions.assertEquals(1, transactionMetricObserver.getShardMetricCache().size());
        Assertions.assertEquals(shardMetricData, transactionMetricObserver.getShardMetricCache().get(context.getShardName()));

        Assertions.assertEquals(1, transactionMetricObserver.getDaoMetricPrefixCache().size());
        Assertions.assertEquals(daoMetricPrefix,
                transactionMetricObserver.getDaoMetricPrefixCache().get(context.getDaoClass()));

        Assertions.assertEquals(1, transactionMetricObserver.getDaoToOpTypeMetricCache().size());
        Assertions.assertEquals(daoMetricData,
                transactionMetricObserver.getDaoToOpTypeMetricCache().get(daoMetricPrefix).get(context.getOpType()));
    }

    private void validateMetrics(final MetricData entityMetricData,
                                 final MetricData shardMetricData,
                                 final MetricData daoMetricData,
                                 final int successCount,
                                 final int failedCount) {
        Assertions.assertEquals(1, entityMetricData.getTotal().getCount());
        Assertions.assertEquals(1, shardMetricData.getTotal().getCount());
        Assertions.assertEquals(1, daoMetricData.getTotal().getCount());

        Assertions.assertEquals(1, entityMetricData.getTimer().getCount());
        Assertions.assertEquals(1, shardMetricData.getTimer().getCount());
        Assertions.assertEquals(1, daoMetricData.getTimer().getCount());

        Assertions.assertEquals(successCount, entityMetricData.getSuccess().getCount());
        Assertions.assertEquals(successCount, shardMetricData.getSuccess().getCount());
        Assertions.assertEquals(successCount, daoMetricData.getSuccess().getCount());

        Assertions.assertEquals(failedCount, entityMetricData.getFailed().getCount());
        Assertions.assertEquals(failedCount, shardMetricData.getFailed().getCount());
        Assertions.assertEquals(failedCount, daoMetricData.getFailed().getCount());
    }

    private Integer terminate() {
         return 1;
    }

    private Integer terminateWithException() {
        throw new RuntimeException();
    }
}
