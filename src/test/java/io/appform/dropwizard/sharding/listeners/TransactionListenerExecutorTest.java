package io.appform.dropwizard.sharding.listeners;

import com.google.common.collect.Lists;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TransactionListenerExecutorTest {

    private TransactionListener transactionListener;
    private TransactionListenerExecutor transactionListenerExecutor;

    @Before
    public void setup() {
        this.transactionListener = Mockito.mock(TransactionListener.class);
        this.transactionListenerExecutor = new TransactionListenerExecutor(Lists.newArrayList(transactionListener));
    }

    @Test
    public void testBeforeExecute() {
        val listenerContext = TransactionListenerContext.builder().build();
        Mockito.doNothing().when(transactionListener).beforeExecute(listenerContext);

        try {
            transactionListenerExecutor.beforeExecute(TransactionListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).beforeExecute(listenerContext);
        try {
            transactionListenerExecutor.beforeExecute(TransactionListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }

    @Test
    public void testAfterExecute() {
        val listenerContext = TransactionListenerContext.builder().build();
        Mockito.doNothing().when(transactionListener).afterExecute(listenerContext);

        try {
            transactionListenerExecutor.afterExecute(TransactionListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).afterExecute(listenerContext);
        try {
            transactionListenerExecutor.afterExecute(TransactionListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }

    @Test
    public void testAfterException() {
        val listenerContext = TransactionListenerContext.builder().build();
        val exception = new RuntimeException();
        Mockito.doNothing().when(transactionListener).afterException(listenerContext, exception);

        try {
            transactionListenerExecutor.afterException(TransactionListenerContext.builder().build(),
                    exception);
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).afterException(listenerContext, exception);
        try {
            transactionListenerExecutor.afterException(TransactionListenerContext.builder().build(), exception);
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }
}
