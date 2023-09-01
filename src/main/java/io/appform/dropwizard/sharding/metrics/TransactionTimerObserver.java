package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import lombok.val;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class TransactionTimerObserver extends TransactionObserver {
    private final TransactionMetricManager metricManager;
    private final Map<Class<?>, Timer> entityTimers = new ConcurrentHashMap<>();
    private final Map<String, Timer> shardTimers = new ConcurrentHashMap<>();

    public TransactionTimerObserver(final TransactionMetricManager metricManager) {
        super(null);
        this.metricManager = metricManager;
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        if(!metricManager.isMetricApplicable(context.getEntityClass())) {
            return proceed(context, supplier);
        }
        val entityTimer = entityTimers.computeIfAbsent(context.getEntityClass(),
                key -> getTimer(context.getEntityClass().getCanonicalName()));
        val shardTimer = shardTimers.computeIfAbsent(context.getShardName(),
                key -> getTimer(context.getShardName()));

        val entityTimerContext = entityTimer.time();
        val shardTimerContext = shardTimer.time();
        try {
            return proceed(context, supplier);
        } finally {
            entityTimerContext.stop();
            shardTimerContext.stop();
        }
    }

    private Timer getTimer(final String metric) {
        return metricManager.getTimer(MetricRegistry.name(metricManager.getMetricPrefix(metric)));
    }
}
