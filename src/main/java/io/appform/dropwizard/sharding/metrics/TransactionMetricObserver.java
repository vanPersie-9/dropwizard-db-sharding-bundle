package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TransactionTimerObserver extends TransactionObserver {
    private final TransactionMetricManager metricManager;
    private final Map<Class<?>, MetricData> entityTimers = new ConcurrentHashMap<>();
    private final Map<String, MetricData> shardTimers = new ConcurrentHashMap<>();

    public TransactionTimerObserver(final TransactionMetricManager metricManager) {
        super(null);
        this.metricManager = metricManager;
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        if (!metricManager.isMetricApplicable(context.getEntityClass())) {
            return proceed(context, supplier);
        }
        val metrics = getMetrics(context);

        val timerContexts = metrics.stream().map(metric -> metric.getTimer().time()).collect(Collectors.toList());
        metrics.forEach(metric -> metric.getTotal().mark());

        try {
            val response = proceed(context, supplier);
            metrics.forEach(metric -> metric.getSuccess().mark());
            return response;
        } catch (Throwable t) {
            metrics.forEach(metric -> metric.getFailed().mark());
            throw t;
        } finally {
            timerContexts.forEach(Timer.Context::stop);
        }
    }

    private List<MetricData> getMetrics(final TransactionExecutionContext context) {
        val entityMetricData = entityTimers.computeIfAbsent(context.getEntityClass(),
                key -> getMetricData(context.getEntityClass().getCanonicalName()));
        val shardMetricData = shardTimers.computeIfAbsent(context.getShardName(),
                key -> getMetricData(context.getShardName()));
        return Lists.newArrayList(entityMetricData, shardMetricData);
    }

    private MetricData getMetricData(final String metric) {
        val metricPrefix = metricManager.getMetricPrefix(metric);
        return MetricData.builder()
                .timer(metricManager.getTimer(MetricRegistry.name(metricPrefix, "latency")))
                .success(metricManager.getMeter(MetricRegistry.name(metricPrefix, "success")))
                .failed(metricManager.getMeter(MetricRegistry.name(metricPrefix, "failed")))
                .total(metricManager.getMeter(MetricRegistry.name(metricPrefix, "total")))
                .build();
    }
}
