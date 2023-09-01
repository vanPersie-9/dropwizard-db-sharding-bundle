package io.appform.dropwizard.sharding.metrics;

import com.codahale.metrics.Meter;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
import lombok.Builder;
import lombok.Value;
import lombok.val;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionCounterListener implements TransactionListener {

    private final Map<Class<?>, CounterMeter> entityRequests = new ConcurrentHashMap<>();
    private final Map<String, CounterMeter> shardRequests = new ConcurrentHashMap<>();
    private final TransactionMetricManager metricManager;

    public TransactionCounterListener(final TransactionMetricManager metricManager) {
        this.metricManager = metricManager;
    }

    @Override
    public void beforeExecute(TransactionExecutionContext listenerContext) {
        if (!metricManager.isMetricApplicable(listenerContext.getEntityClass())) {
            return;
        }
        entityRequests.computeIfAbsent(listenerContext.getEntityClass(),
                        key -> getCounterMeter(listenerContext.getEntityClass().getCanonicalName()))
                .getTotal().mark();

        shardRequests.computeIfAbsent(listenerContext.getShardName(),
                key -> getCounterMeter(listenerContext.getShardName())).getTotal().mark();
    }

    @Override
    public void afterExecute(TransactionExecutionContext listenerContext) {
        if (!metricManager.isMetricApplicable(listenerContext.getEntityClass())) {
            return;
        }
        entityRequests.get(listenerContext.getEntityClass()).getSuccess().mark();
        shardRequests.get(listenerContext.getShardName()).getSuccess().mark();
    }

    @Override
    public void afterException(TransactionExecutionContext listenerContext, Throwable e) {
        if (!metricManager.isMetricApplicable(listenerContext.getEntityClass())) {
            return;
        }
        entityRequests.get(listenerContext.getEntityClass()).getFailed().mark();
        shardRequests.get(listenerContext.getShardName()).getFailed().mark();
    }

    private CounterMeter getCounterMeter(final String metric) {
        val metricPrefix = metricManager.getMetricPrefix(metric);
        return CounterMeter.builder()
                .total(metricManager.getMeter(metricPrefix + "total"))
                .success(metricManager.getMeter(metricPrefix + "success"))
                .failed(metricManager.getMeter(metricPrefix + "failed"))
                .build();
    }

    @Builder
    @Value
    private static class CounterMeter {
        Meter total;
        Meter success;
        Meter failed;
    }
}
