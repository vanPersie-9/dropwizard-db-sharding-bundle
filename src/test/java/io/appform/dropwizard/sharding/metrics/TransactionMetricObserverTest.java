package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.Timer;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TransactionTimerObserverTest {

    private TransactionTimerObserver transactionTimerObserver;
    private TransactionMetricManager metricManager;

    @Before
    public void setup() {
        this.metricManager = Mockito.mock(TransactionMetricManager.class);
        this.transactionTimerObserver = new TransactionTimerObserver(metricManager);
    }

    @Test
    public void testExecuteWhenMetricNotApplicable() {
        Mockito.doReturn(false).when(metricManager).isMetricApplicable(null);
        Assert.assertEquals(terminate(),
                transactionTimerObserver.execute(TransactionExecutionContext.builder().build(), this::terminate));
    }

    @Test
    public void testExecuteWhenMetricApplicable() {
        val context = TransactionExecutionContext.builder()
                .entityClass(this.getClass())
                .shardName("shard")
                .build();
        val entityTimer = new Timer();
        val shardTimer = new Timer();

        Mockito.doReturn(context.getEntityClass().getCanonicalName()).when(metricManager)
                .getMetricPrefix(context.getEntityClass().getCanonicalName());
        Mockito.doReturn(context.getShardName()).when(metricManager).getMetricPrefix(context.getShardName());

        Mockito.doReturn(true).when(metricManager).isMetricApplicable(context.getEntityClass());
        Mockito.doReturn(entityTimer).when(metricManager).getTimer(context.getEntityClass().getCanonicalName() + "latency");
        Mockito.doReturn(shardTimer).when(metricManager).getTimer(context.getShardName() + "latency");
        Assert.assertEquals(terminate(), transactionTimerObserver.execute(context, this::terminate));
        Assert.assertEquals(1, entityTimer.getCount());
        Assert.assertEquals(1, shardTimer.getCount());
    }

    private Integer terminate() {
         return 1;
    }
}
