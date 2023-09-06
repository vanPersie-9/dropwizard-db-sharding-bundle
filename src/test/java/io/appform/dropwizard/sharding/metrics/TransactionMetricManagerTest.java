package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import io.appform.dropwizard.sharding.config.MetricConfig;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class TransactionMetricManagerTest {

    private final TransactionMetricManager metricManager = new TransactionMetricManager();

    @Test
    public void testIsMetricApplicable() {
        metricManager.initialize(null, null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(() -> null, null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(() -> MetricConfig.builder().enabledForAll(true).build(), null);
        Assert.assertTrue(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(() -> MetricConfig.builder().enabledForAll(false)
                .enabledForEntities(ImmutableSet.of(this.getClass().getCanonicalName()))
                .build(), null);
        Assert.assertTrue(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(() -> MetricConfig.builder().enabledForAll(false)
                .enabledForEntities(ImmutableSet.of(this.getClass().getSimpleName()))
                .build(), null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(() -> MetricConfig.builder().enabledForAll(false)
                .build(), null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));
    }

    @Test
    public void testGetDaoMetricPrefix() {
        val metricPrefix = metricManager.getDaoMetricPrefix(this.getClass());
        Assert.assertEquals("db.sharding.operation.io_appform_dropwizard_sharding_metrics_TransactionMetricManagerTest",
                metricPrefix);
    }

    @Test
    public void testGetDaoOpMetricData() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(null, metricRegistry);
        val metricPrefix = "test";
        val context = TransactionExecutionContext.builder()
                .opType("save")
                .build();
        val metricData = metricManager.getDaoOpMetricData(metricPrefix, context);
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(4, metrics.size());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.latency"), metricData.getTimer());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.total"), metricData.getTotal());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.success"), metricData.getSuccess());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.failed"), metricData.getFailed());
    }

    @Test
    public void testGetDaoOpMetricDataWithLockedContextMode() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(null, metricRegistry);
        val metricPrefix = "test";
        val context = TransactionExecutionContext.builder()
                .opType("save")
                .lockedContextMode("read")
                .build();
        val metricData = metricManager.getDaoOpMetricData(metricPrefix, context);
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(4, metrics.size());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.read.latency"), metricData.getTimer());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.read.total"), metricData.getTotal());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.read.success"), metricData.getSuccess());
        Assert.assertEquals(metrics.get(metricPrefix + "." + "save.read.failed"), metricData.getFailed());
    }

    @Test
    public void testGetShardMetricData() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(null, metricRegistry);
        val shardName = "test";
        val metricData = metricManager.getShardMetricData(shardName);
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(4, metrics.size());
        val metricPrefix = "db.sharding.shard." + shardName + ".";
        Assert.assertEquals(metrics.get(metricPrefix + "latency"), metricData.getTimer());
        Assert.assertEquals(metrics.get(metricPrefix + "total"), metricData.getTotal());
        Assert.assertEquals(metrics.get(metricPrefix + "success"), metricData.getSuccess());
        Assert.assertEquals(metrics.get(metricPrefix + "failed"), metricData.getFailed());
    }

    @Test
    public void testGetEntityMetricData() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(null, metricRegistry);
        val entityClass = this.getClass();
        val metricData = metricManager.getEntityMetricData(entityClass);
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(4, metrics.size());

        val metricPrefix = "db.sharding.entity.io_appform_dropwizard_sharding_metrics_TransactionMetricManagerTest.";
        Assert.assertEquals(metrics.get(metricPrefix + "latency"), metricData.getTimer());
        Assert.assertEquals(metrics.get(metricPrefix + "total"), metricData.getTotal());
        Assert.assertEquals(metrics.get(metricPrefix + "success"), metricData.getSuccess());
        Assert.assertEquals(metrics.get(metricPrefix + "failed"), metricData.getFailed());
    }
}
