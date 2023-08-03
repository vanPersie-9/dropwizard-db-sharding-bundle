package io.appform.dropwizard.sharding.listeners;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class TransactionListenerExecutor {

    private final List<TransactionListener> transactionListeners;

    public TransactionListenerExecutor(final List<TransactionListener> transactionListeners) {
        this.transactionListeners = transactionListeners;
    }

    public TransactionListenerExecutor() {
        this(ImmutableList.of());
    }

    public void beforeExecute(final TransactionListenerContext listenerContext) {
        if (transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.beforeExecute(listenerContext);
            } catch (Throwable e) {
                log.error("Error running before execute of listener: " + transactionListener.getClass(), e);
            }
        });
    }

    public void afterExecute(final TransactionListenerContext listenerContext) {
        if (transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.afterExecute(listenerContext);
            } catch (Throwable e) {
                log.error("Error running after execute of listener: " + transactionListener.getClass(), e);
            }
        });
    }

    public void afterException(final TransactionListenerContext listenerContext,
                               final Throwable throwable) {
        if (transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.afterException(listenerContext, throwable);
            } catch (Throwable e) {
                log.error("Error running after exception of listener: " + transactionListener.getClass(), e);
            }
        });
    }
}
