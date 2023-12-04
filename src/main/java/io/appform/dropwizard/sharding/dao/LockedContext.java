package io.appform.dropwizard.sharding.dao;

import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.dao.operations.OpContext;
import io.appform.dropwizard.sharding.dao.operations.lockedcontext.LockAndExecute;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.utils.TransactionHandler;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;

@Getter
public class LockedContext<T> {

    @FunctionalInterface
    public interface Mutator<T> {
        void mutator(T parent);
    }

    private final int shardId;
    private final SessionFactory sessionFactory;
    private final TransactionExecutionContext executionContext;
    private final TransactionObserver observer;

    public LockedContext(
            int shardId,
            SessionFactory sessionFactory,
            Supplier<T> getter,
            Class<T> entityClass,
            ShardInfoProvider shardInfoProvider,
            TransactionObserver observer) {
        this.shardId = shardId;
        this.sessionFactory = sessionFactory;
        this.observer = observer;
        OpContext opContext = LockAndExecute.<T>builder().getter(getter).mode(Mode.READ).build();
        this.executionContext = buildExecutionContext(shardInfoProvider, entityClass, opContext);
    }

    public LockedContext(
            int shardId,
            SessionFactory sessionFactory,
            Function<T, T> saver,
            T entity,
            Class<T> entityClass,
            ShardInfoProvider shardInfoProvider,
            TransactionObserver observer) {
        this.shardId = shardId;
        this.sessionFactory = sessionFactory;
        this.observer = observer;
        OpContext opContext = LockAndExecute.<T>builder().saver(saver).mode(Mode.INSERT)
            .entity(entity).build();
        this.executionContext = buildExecutionContext(shardInfoProvider, entityClass, opContext);
    }

    public LockedContext<T> apply(Consumer<T> handler) {
        ((LockAndExecute) this.executionContext.getOpContext()).getOperations().add(handler);
        return this;
    }

    public LockedContext<T> mutate(Mutator<T> mutator) {
        return apply(parent -> mutator.mutator(parent));
    }

    public <U> LockedContext<T> save(RelationalDao<U> relationalDao,
        Function<T, U> entityGenerator) {
        return apply(parent -> {
            try {
                U entity = entityGenerator.apply(parent);
                relationalDao.save(this, entity);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public T execute() {
        return observer.execute(executionContext, () -> {
            TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, false);
            transactionHandler.beforeStart();
            try {
                OpContext<T> opContext = ((LockAndExecute<T>) executionContext.getOpContext());
               return opContext.apply(transactionHandler.getSession());
            } catch (Exception e) {
                transactionHandler.onError();
                throw e;
            } finally {
                transactionHandler.afterEnd();
            }
        });
    }

    public <U> LockedContext<T> saveAll(RelationalDao<U> relationalDao, Function<T, List<U>> entityGenerator) {
        return apply(parent -> {
            try {
                List<U> entities = entityGenerator.apply(parent);
                for (U entity : entities) {
                    relationalDao.save(this, entity);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> save(RelationalDao<U> relationalDao, U entity, Function<U, U> handler) {
        return apply(parent -> {
            try {
                relationalDao.save(this, entity, handler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> updateUsingQuery(
            RelationalDao<U> relationalDao,
            UpdateOperationMeta updateOperationMeta) {
        return apply(parent -> {
            try {
                relationalDao.updateUsingQuery(this, updateOperationMeta);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> update(RelationalDao<U> relationalDao, Object id, Function<U, U> handler) {
        return apply(parent -> {
            try {
                relationalDao.update(this, id, handler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> createOrUpdate(
            RelationalDao<U> relationalDao,
            DetachedCriteria criteria,
            UnaryOperator<U> updater,
            Supplier<U> entityGenerator) {
        return apply(parent -> {
            try {
                relationalDao.createOrUpdate(this, criteria, updater, parent, p -> entityGenerator.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> createOrUpdate(
            RelationalDao<U> relationalDao,
            DetachedCriteria criteria,
            UnaryOperator<U> updater,
            Function<T, U> entityGenerator) {
        return apply(parent -> {
            try {
                relationalDao.createOrUpdate(this, criteria, updater, parent, entityGenerator);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public <U> LockedContext<T> update(
            RelationalDao<U> relationalDao,
            DetachedCriteria criteria,
            UnaryOperator<U> updater,
            BooleanSupplier updateNext) {
        return apply(parent -> {
            try {
                relationalDao.update(this, criteria, updater, updateNext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public LockedContext<T> filter(Predicate<T> predicate) {
        return filter(predicate, new IllegalArgumentException("Predicate check failed"));
    }

    public LockedContext<T> filter(Predicate<T> predicate, RuntimeException failureException) {
        return apply(parent -> {
            boolean result = predicate.test(parent);
            if (!result) {
                throw failureException;
            }
        });
    }

    private TransactionExecutionContext buildExecutionContext(
        final ShardInfoProvider shardInfoProvider,
        final Class<T> entityClass, final OpContext opContext) {
        return TransactionExecutionContext.builder()
            .commandName("execute")
            .shardName(shardInfoProvider.shardName(shardId))
            .entityClass(entityClass)
            .daoClass(getClass())
            .opContext(opContext)
            .build();
    }

    public enum Mode {READ, INSERT}

}