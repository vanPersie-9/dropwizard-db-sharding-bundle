package io.appform.dropwizard.sharding.observers;

import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
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
            listener.forEach(interceptor -> {
                try {
                    interceptor.beforeExecute(context);
                }
                catch (Throwable t) {
                    log.info("Error running listener beforeExecute: " + interceptor.getClass(), t);
                }
            });
            val result = proceed(context, supplier);
            listener.forEach(interceptor -> {
                try {
                    interceptor.afterExecute(context);
                }
                catch (Throwable t) {
                    log.info("Error running listener afterExecute: " + interceptor.getClass(), t);
                }

            });
            return result;
        }
        catch (Throwable t) {
            listener.forEach(interceptor -> {
                try {
                    interceptor.afterException(context, t);
                }
                catch (Throwable th) {
                    log.info("Error running listener afterException: " + interceptor.getClass(), th);
                }

            });
            throw t;
        }
    }
}
