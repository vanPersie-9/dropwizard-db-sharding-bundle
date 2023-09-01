package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;
import io.appform.dropwizard.sharding.config.MetricConfig;

import java.util.concurrent.TimeUnit;


public class TransactionMetricManager {

    private MetricConfig metricConfig;
    private MetricRegistry metricRegistry;
    private static final String METRIC_PREFIX = "db.sharding";
    private static final String DELIMITER = ".";
    private static final String DELIMITER_REPLACEMENT = "_";

    public void initialize(final MetricConfig metricConfig,
                           final MetricRegistry metricRegistry) {
        this.metricConfig = metricConfig;
        this.metricRegistry = metricRegistry;
    }

    public boolean isMetricApplicable(final Class<?> entityClass) {
        if(metricConfig == null) {
            return false;
        }

        if(metricConfig.isEnabledForAll()) {
            return true;
        }

        return metricConfig.getEnabledForEntities() != null
                && metricConfig.getEnabledForEntities().contains(entityClass.getCanonicalName());
    }

    public Meter getMeter(final String name) {
        return metricRegistry.meter(name);
    }

    public Timer getTimer(final String metric) {
        return metricRegistry.timer(metric, () ->
                new Timer(new SlidingTimeWindowArrayReservoir(60, TimeUnit.SECONDS)));
    }

    public String getMetricPrefix(final String metric) {
        return METRIC_PREFIX + DELIMITER + normalizeString(metric) + DELIMITER;
    }

    private String normalizeString(final String name) {
        return name.replace(DELIMITER, DELIMITER_REPLACEMENT);
    }

}
