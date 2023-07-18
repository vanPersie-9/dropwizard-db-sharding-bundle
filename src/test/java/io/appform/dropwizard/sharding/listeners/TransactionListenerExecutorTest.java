package io.appform.dropwizard.sharding.listeners;

import com.google.common.collect.Lists;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TransactionListenerExecutorTest {

    private TransactionListener transactionListener;

    @Before
    public void setup() {
        this.transactionListener = Mockito.mock(TransactionListener.class);
    }

    @Test
    public void testBeforeExecute() {
        val listenerContext = ListenerContext.builder().build();
        Mockito.doNothing().when(transactionListener).beforeExecute(listenerContext);

        try {
            TransactionListenerExecutor.beforeExecute(Lists.newArrayList(transactionListener), ListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).beforeExecute(listenerContext);
        try {
            TransactionListenerExecutor.beforeExecute(Lists.newArrayList(transactionListener), ListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }

    @Test
    public void testAfterExecute() {
        val listenerContext = ListenerContext.builder().build();
        Mockito.doNothing().when(transactionListener).afterExecute(listenerContext);

        try {
            TransactionListenerExecutor.afterExecute(Lists.newArrayList(transactionListener), ListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).afterExecute(listenerContext);
        try {
            TransactionListenerExecutor.afterExecute(Lists.newArrayList(transactionListener), ListenerContext.builder().build());
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }

    @Test
    public void testAfterException() {
        val listenerContext = ListenerContext.builder().build();
        val exception = new RuntimeException();
        Mockito.doNothing().when(transactionListener).afterException(listenerContext, exception);

        try {
            TransactionListenerExecutor.afterException(Lists.newArrayList(transactionListener), ListenerContext.builder().build(),
                    exception);
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }

        Mockito.doThrow(new RuntimeException()).when(transactionListener).afterException(listenerContext, exception);
        try {
            TransactionListenerExecutor.afterException(Lists.newArrayList(transactionListener), ListenerContext.builder().build(),
                    exception);
        } catch (Exception e) {
            Assert.fail("Exception was not expected");
        }
    }
}
