package io.appform.dropwizard.sharding.observers.internal;

import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 *
 */
@Slf4j
@Getter
public class ListenerTriggeringObserver extends TransactionObserver {
    private final List<TransactionListener> listener = new ArrayList<>();

    public ListenerTriggeringObserver() {
        this(null);
    }

    public ListenerTriggeringObserver(TransactionObserver next) {
        super(next);
    }


    public TransactionObserver addListener(final TransactionListener listener) {
        this.listener.add(listener);
        return this;
    }

    public TransactionObserver addListeners(final Collection<TransactionListener> listeners) {
        listener.addAll(listeners);
        return this;
    }

    @Override
    public <T> T execute(TransactionExecutionContext context, Supplier<T> supplier) {
        Objects.requireNonNull(context, "Context cannot be null");
        try {
            listener.forEach(listener -> {
                try {
                    listener.beforeExecute(context);
                }
                catch (Throwable t) {
                    log.info("Error running listener beforeExecute: " + listener.getClass(), t);
                }
            });
            val result = proceed(context, supplier);
            listener.forEach(listener -> {
                try {
                    listener.afterExecute(context);
                }
                catch (Throwable t) {
                    log.info("Error running listener afterExecute: " + listener.getClass(), t);
                }

            });
            return result;
        }
        catch (Throwable t) {
            listener.forEach(listener -> {
                try {
                    listener.afterException(context, t);
                }
                catch (Throwable th) {
                    log.info("Error running listener afterException: " + listener.getClass(), th);
                }

            });
            throw t;
        }
    }
}
