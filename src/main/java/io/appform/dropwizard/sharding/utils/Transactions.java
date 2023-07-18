/*
 * Copyright 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.sharding.utils;

import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.listeners.ListenerContext;
import io.appform.dropwizard.sharding.listeners.TransactionListener;
import io.appform.dropwizard.sharding.listeners.TransactionListenerExecutor;
import io.appform.dropwizard.sharding.listeners.TransactionListenerFactory;
import lombok.val;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility functional class for running transactions.
 */
public class Transactions {

    private final Class<?> daoClass;
    private final Class<?> entityClass;
    private final Map<Integer, List<TransactionListener>> transactionListeners;
    private final ShardInfoProvider shardInfoProvider;

    public Transactions(final ShardInfoProvider shardInfoProvider,
                        final Class<?> daoClass,
                        final Class<?> entityClass,
                        final List<TransactionListenerFactory> transactionListenerFactories,
                        final int shards) {
        this.daoClass = daoClass;
        this.entityClass = entityClass;
        this.transactionListeners = IntStream.range(0, shards)
                .boxed()
                .collect(Collectors.toMap(shardId -> shardId, shardId -> {
                    val shardName = shardInfoProvider.shardName(shardId);
                    return transactionListenerFactories.stream().map(listenerFactory ->
                            listenerFactory.createListener(daoClass, entityClass, shardName)).collect(Collectors.toList());
                }));
        this.shardInfoProvider = shardInfoProvider;
    }

    public <T, U> Optional<T> executeAndResolve(SessionFactory sessionFactory, Function<U, T> function, U arg,
                                                String opType,
                                                int shardId) {
        return executeAndResolve(sessionFactory, false, function, arg, opType, shardId);
    }

    public <T, U> Optional<T> executeAndResolve(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg,
                                                String opType,
                                                int shardId) {
        T result = execute(sessionFactory, readOnly, function, arg, opType, shardId);
        return Optional.ofNullable(result);
    }

    public <T, U> T execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg,
                            String opType,
                            int shardId) {
        return execute(sessionFactory, readOnly, function, arg, t -> t, opType, shardId);
    }

    public <T, U> T execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg, boolean completeTransaction,
                            String opType,
                            int shardId) {
        return execute(sessionFactory, readOnly, function, arg, t -> t, completeTransaction, opType, shardId);
    }

    public <T, U, V> V execute(SessionFactory sessionFactory, boolean readOnly, Function<U, T> function, U arg, Function<T, V> handler,
                               String opType,
                               int shardId) {
        return execute(sessionFactory, readOnly, function, arg, handler, true, opType, shardId);
    }

    public <T, U, V> V execute(SessionFactory sessionFactory,
                               boolean readOnly,
                               Function<U, T> function,
                               U arg,
                               Function<T, V> handler,
                               boolean completeTransaction,
                               String opType,
                               int shardId) {
        val listenerContext = ListenerContext.builder()
                .entityClass(entityClass)
                .daoClass(daoClass)
                .opType(opType)
                .shardName(shardInfoProvider.shardName(shardId))
                .build();
        val listeners = transactionListeners.get(shardId);
        TransactionListenerExecutor.beforeExecute(listeners, listenerContext);
        TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, readOnly);
        if (completeTransaction) {
            transactionHandler.beforeStart();
        }
        try {
            T result = function.apply(arg);
            V returnValue = handler.apply(result);
            if (completeTransaction) {
                transactionHandler.afterEnd();
            }
            TransactionListenerExecutor.afterExecute(listeners, listenerContext);
            return returnValue;
        } catch (Exception e) {
            if (completeTransaction) {
                transactionHandler.onError();
            }
            TransactionListenerExecutor.afterException(listeners, listenerContext, e);
            throw e;
        }
    }

    public <T> T execute(SessionFactory sessionFactory, Function<Session, T> handler, String opType, int shardId) {
        val listenerContext = ListenerContext.builder()
                .entityClass(entityClass)
                .daoClass(daoClass)
                .opType(opType)
                .shardName(shardInfoProvider.shardName(shardId))
                .build();
        val listeners = transactionListeners.get(shardId);
        TransactionListenerExecutor.beforeExecute(listeners, listenerContext);
        TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, true);
        transactionHandler.beforeStart();
        try {
            T result = handler.apply(transactionHandler.getSession());
            transactionHandler.afterEnd();
            TransactionListenerExecutor.afterExecute(listeners, listenerContext);
            return result;
        } catch (Exception e) {
            transactionHandler.onError();
            TransactionListenerExecutor.afterException(listeners, listenerContext, e);
            throw e;
        }
    }
}
