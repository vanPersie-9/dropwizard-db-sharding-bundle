package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import io.appform.dropwizard.sharding.config.MetricConfig;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class TransactionMetricManagerTest {

    private final TransactionMetricManager metricManager = new TransactionMetricManager();

    @Test
    public void testIsMetricApplicable() {
        metricManager.initialize(null, null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(MetricConfig.builder().enabledForAll(true).build(), null);
        Assert.assertTrue(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(MetricConfig.builder().enabledForAll(false)
                .enabledForEntities(ImmutableSet.of(this.getClass().getCanonicalName()))
                .build(), null);
        Assert.assertTrue(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(MetricConfig.builder().enabledForAll(false)
                .enabledForEntities(ImmutableSet.of(this.getClass().getSimpleName()))
                .build(), null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));

        metricManager.initialize(MetricConfig.builder().enabledForAll(false)
                .build(), null);
        Assert.assertFalse(metricManager.isMetricApplicable(this.getClass()));
    }

    @Test
    public void testGetMetricPrefix() {
        val metric = "test.abc";
        val metricPrefix = metricManager.getMetricPrefix(metric);
        Assert.assertEquals("db.sharding.test_abc.", metricPrefix);
    }

    @Test
    public void testGetMeter() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(MetricConfig.builder().build(), metricRegistry);
        val meter = metricManager.getMeter("test");
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(1, metrics.size());
        Assert.assertEquals(meter, metrics.get("test"));
    }

    @Test
    public void testGetTimer() {
        val metricRegistry = new MetricRegistry();
        metricManager.initialize(MetricConfig.builder().build(), metricRegistry);
        val meter = metricManager.getTimer("test");
        val metrics = metricRegistry.getMetrics();
        Assert.assertEquals(1, metrics.size());
        Assert.assertEquals(meter, metrics.get("test"));
    }
}
