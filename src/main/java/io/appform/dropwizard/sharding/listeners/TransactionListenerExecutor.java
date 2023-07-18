package io.appform.dropwizard.sharding.listeners;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@UtilityClass
public class TransactionListenerExecutor {

    public void beforeExecute(final List<TransactionListener> transactionListeners,
                              final ListenerContext listenerContext) {
        if(transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.beforeExecute(listenerContext);
            } catch (Exception e) {
                log.error(String.format("Error running before execute of listener: %s", transactionListener.getClass()), e);
            }
        });
    }

    public void afterExecute(final List<TransactionListener> transactionListeners,
                             final ListenerContext listenerContext) {
        if(transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.afterExecute(listenerContext);
            } catch (Exception e) {
                log.error(String.format("Error running after execute of listener: %s", transactionListener.getClass()), e);
            }
        });
    }

    public void afterException(final List<TransactionListener> transactionListeners,
                               final ListenerContext listenerContext,
                               final Throwable throwable) {
        if(transactionListeners == null) {
            return;
        }
        transactionListeners.forEach(transactionListener -> {
            try {
                transactionListener.afterException(listenerContext, throwable);
            } catch (Exception e) {
                log.error(String.format("Error running after exception of listener: %s", transactionListener.getClass()), e);
            }
        });
    }
}
