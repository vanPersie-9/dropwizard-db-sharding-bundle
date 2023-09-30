package io.appform.dropwizard.sharding.dao;

import com.google.common.collect.Lists;
import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.execution.TransactionExecutionContext;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.query.QuerySpec;
import io.appform.dropwizard.sharding.utils.TransactionHandler;
import lombok.Getter;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.DetachedCriteria;

import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * The `LockedContext` class encapsulates the context for locked operations on an entity in a specific shard.
 * It provides various methods for applying operations and actions to the entity within the context of a transaction.
 * This context can be used for both reading and inserting entities.
 *
 * @param <T> The type of the entity on which the operations are performed.
 */
@Getter
public class LockedContext<T> {
    @FunctionalInterface
    public interface Mutator<T> {
        void mutator(T parent);
    }

    enum Mode {READ, INSERT}

    private final int shardId;
    private final SessionFactory sessionFactory;
    private final List<Function<T, Void>> operations = Lists.newArrayList();
    private Supplier<T> getter;
    private Function<T, T> saver;
    private T entity;
    private final Mode mode;
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
        this.getter = getter;
        this.observer = observer;
        this.mode = Mode.READ;
        this.executionContext = buildExecutionContext(shardInfoProvider, entityClass);
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
        this.saver = saver;
        this.entity = entity;
        this.observer = observer;
        this.mode = Mode.INSERT;
        this.executionContext = buildExecutionContext(shardInfoProvider, entityClass);
    }


    /**
     * Applies a custom mutator function to the parent entity within this locked context. The mutator function is responsible
     * for making modifications to the parent entity based on specific business logic.
     *
     * @param mutator The mutator function that defines how to modify the parent entity.
     * @return A reference to this `LockedContext` to allow method chaining.
     */
    public LockedContext<T> mutate(Mutator<T> mutator) {
        return apply(parent -> {
            mutator.mutator(parent);
            return null;
        });
    }

    public LockedContext<T> apply(Function<T, Void> handler) {
        this.operations.add(handler);
        return this;
    }

    /**
     * Generates entity of type {@code U} using entityGenerator and then persists them
     *
     * @param <U>             The type of the associated entity to be saved.
     * @param relationalDao   The relational DAO responsible for saving the associated entity.
     * @param entityGenerator A function that generates the associated entity based on the parent entity.
     * @return A reference to this LockedContext, enabling method chaining.
     * @throws RuntimeException         if an exception occurs during entity generation or saving.
     *                                  This exception typically wraps any underlying exceptions that may occur
     *                                  during the execution of the entity generation or save operation.
     *                                  It indicates that the save operation was unsuccessful.
     * @throws IllegalArgumentException if the provided relational DAO or entity generator function is null.
     *                                  This exception indicates invalid or missing inputs.
     */
    public <U> LockedContext<T> save(RelationalDao<U> relationalDao, Function<T, U> entityGenerator) {
        return apply(parent -> {
            try {
                U entity = entityGenerator.apply(parent);
                relationalDao.save(this, entity);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Generates list of entity of type {@code U} using entityGenerator and then persists them in bulk
     *
     * @param <U>             The type of the associated entity to be saved.
     * @param relationalDao   The relational DAO responsible for saving the associated entity.
     * @param entityGenerator A function that generates the associated entity based on the parent entity.
     * @return A reference to this LockedContext, enabling method chaining.
     * @throws RuntimeException         if an exception occurs during entity generation or saving.
     *                                  This exception typically wraps any underlying exceptions that may occur
     *                                  during the execution of the entity generation or save operation.
     *                                  It indicates that the save operation was unsuccessful.
     * @throws IllegalArgumentException if the provided relational DAO or entity generator function is null.
     *                                  This exception indicates invalid or missing inputs.
     */
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
            return null;
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
            return null;
        });
    }

    /**
     * Saves an associated entity using a relational DAO, an associated entity instance, and a handler function.
     *
     * @param <U>           The type of the associated entity to be saved.
     * @param relationalDao The relational DAO responsible for saving the associated entity.
     * @param entity        The associated entity instance to be saved.
     * @param handler       A handler function that can modify the associated entity before saving.
     * @return A reference to this LockedContext, enabling method chaining.
     * @throws RuntimeException         if an exception occurs during the save operation or if the handler function
     *                                  throws an exception. This exception typically wraps any underlying exceptions
     *                                  that may occur during the execution of the save operation or the handler function.
     *                                  It indicates that the save operation was unsuccessful.
     * @throws IllegalArgumentException if the provided relational DAO or associated entity is null. This exception
     *                                  indicates invalid or missing inputs.
     * @implSpec This method allows you to save an associated entity (of type U) using a specified relational DAO,
     * an associated entity instance, and an optional handler function. The handler function can be used
     * to modify the associated entity before it is saved.
     * @apiNote The {@code handler} function, if provided, takes the associated entity (of type U) as input and can
     * apply custom modifications to it before the save operation. The save operation is performed by
     * the provided {@code relationalDao}. If the save operation is successful, this method returns a
     * reference to the current LockedContext, allowing for method chaining. If an exception occurs during
     * the save operation or if the handler function throws an exception, it is wrapped in a RuntimeException,
     * indicating that the save operation was unsuccessful.
     */
    public <U> LockedContext<T> save(RelationalDao<U> relationalDao, U entity, Function<U, U> handler) {
        return apply(parent -> {
            try {
                relationalDao.save(this, entity, handler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Updates an associated entity using a relational DAO, an identifier, and a handler function.
     *
     * @param <U>           The type of the associated entity to be updated.
     * @param relationalDao The relational DAO responsible for updating the associated entity.
     * @param id            The identifier of the associated entity to be updated.
     * @param handler       A handler function that can modify the associated entity before updating.
     * @return A reference to this LockedContext, enabling method chaining.
     * @throws RuntimeException         if an exception occurs during the update operation or if the handler function
     *                                  throws an exception. This exception typically wraps any underlying exceptions
     *                                  that may occur during the execution of the update operation or the handler function.
     *                                  It indicates that the update operation was unsuccessful.
     * @throws IllegalArgumentException if the provided relational DAO or identifier is null. This exception
     *                                  indicates invalid or missing inputs.
     * @apiNote The {@code handler} function, if provided, takes the associated entity (of type U) as input and can
     * apply custom modifications to it before the update operation. The update operation is performed by
     * the provided {@code relationalDao}. If the update operation is successful, this method returns a
     * reference to the current LockedContext, allowing for method chaining. If an exception occurs during
     * the update operation or if the handler function throws an exception, it is wrapped in a RuntimeException,
     * indicating that the update operation was unsuccessful.
     */
    public <U> LockedContext<T> update(RelationalDao<U> relationalDao, Object id, Function<U, U> handler) {
        return apply(parent -> {
            try {
                relationalDao.update(this, id, handler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Queries using the specified criteria across all shards and returns the counts of rows satisfying the criteria.
     * <b>Note:</b> This method runs the query serially and it's usage is not recommended.
     *
     * @param criteria The select criteria
     * @return List of counts in each shard
     */
    public <U> LockedContext<T> createOrUpdate(
            RelationalDao<U> relationalDao,
            DetachedCriteria criteria,
            Function<U, U> updater,
            Supplier<U> entityGenerator) {
        return apply(parent -> {
            try {
                relationalDao.createOrUpdate(this, criteria, updater, entityGenerator);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Creates or updates using this context using the provided relational data access object (DAO),
     * query specification, updater function, and entity generator
     *
     * @param <U>             The type of entity being operated upon by the DAO, updater, and generator.
     * @param relationalDao   The relational data access object responsible for performing the create
     *                        or update operation on the entity.
     * @param querySpec       The query specification that defines the criteria for locating the entity
     *                        to be created or updated.
     * @param updater         A function that specifies how to update the entity if it already exists.
     *                        It takes an existing entity as input and returns the updated entity.
     * @param entityGenerator A supplier function that provides a new entity to be created if the entity
     *                        specified by the query specification does not exist.
     * @return A LockedContext<T> representing the result of the create or update operation.
     * @throws RuntimeException If an exception occurs during the create or update operation, it is wrapped
     *                          in a RuntimeException and thrown.
     */
    public <U> LockedContext<T> createOrUpdate(
            RelationalDao<U> relationalDao,
            QuerySpec<U, U> querySpec,
            Function<U, U> updater,
            Supplier<U> entityGenerator) {
        return apply(parent -> {
            try {
                relationalDao.createOrUpdate(this, querySpec, updater, entityGenerator);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Updates entities in the context using the provided relational data access object (DAO),
     * criteria for selecting entities, an updater function, and a boolean supplier for
     * determining whether to continue updating the next matching entity.
     *
     * @param <U>           The type of entity being operated upon by the DAO and updater.
     * @param relationalDao The relational data access object responsible for performing the update
     *                      operation on the selected entities.
     * @param criteria      The criteria that define which entities should be updated. This can be a
     *                      specification of the conditions that entities must meet to be considered
     *                      for updating.
     * @param updater       A function that specifies how to update an entity. It takes an existing
     *                      entity as input and returns the updated entity.
     * @param updateNext    A boolean supplier that determines whether to continue updating the next
     *                      matching entity. If this supplier returns true, the update operation
     *                      continues to the next matching entity; if it returns false, the operation
     *                      stops.
     * @return A LockedContext<T> representing the result of the update operation
     * @throws RuntimeException If an exception occurs during the update operation, it is wrapped
     *                          in a RuntimeException and thrown.
     */
    public <U> LockedContext<T> update(
            RelationalDao<U> relationalDao,
            DetachedCriteria criteria,
            Function<U, U> updater,
            BooleanSupplier updateNext) {
        return apply(parent -> {
            try {
                relationalDao.update(this, criteria, updater, updateNext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    /**
     * Updates entities in the context using the provided relational data access object (DAO),
     * query specification for selecting entities, an updater function, and a boolean supplier
     * for determining whether to continue updating the next matching entity.
     *
     * @param <U>           The type of entity being operated upon by the DAO and updater.
     * @param relationalDao The relational data access object responsible for performing the update
     *                      operation on the selected entities.
     * @param criteria      The query specification that defines which entities should be updated.
     *                      This specification typically includes conditions and filters to select
     *                      the entities to be updated.
     * @param updater       A function that specifies how to update an entity. It takes an existing
     *                      entity as input and returns the updated entity.
     * @param updateNext    A boolean supplier that determines whether to continue updating the next
     *                      matching entity. If this supplier returns true, the update operation
     *                      continues to the next matching entity; if it returns false, the operation
     *                      stops.
     * @return A LockedContext<T> representing the result of the update operation.
     * @throws RuntimeException If an exception occurs during the update operation, it is wrapped
     *                          in a RuntimeException and thrown.
     */
    public <U> LockedContext<T> update(
            RelationalDao<U> relationalDao,
            QuerySpec<U, U> criteria,
            Function<U, U> updater,
            BooleanSupplier updateNext) {
        return apply(parent -> {
            try {
                relationalDao.update(this, criteria, updater, updateNext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
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
            return null;
        });
    }

    public T execute() {
        return observer.execute(executionContext, () -> {
            TransactionHandler transactionHandler = new TransactionHandler(sessionFactory, false);
            transactionHandler.beforeStart();
            try {
                T result = generateEntity();
                operations
                        .forEach(operation -> operation.apply(result));
                return result;
            } catch (Exception e) {
                transactionHandler.onError();
                throw e;
            } finally {
                transactionHandler.afterEnd();
            }
        });
    }

    private TransactionExecutionContext buildExecutionContext(final ShardInfoProvider shardInfoProvider,
                                                              final Class<T> entityClass) {
        return TransactionExecutionContext.builder()
                .shardName(shardInfoProvider.shardName(shardId))
                .lockedContextMode(mode.name())
                .entityClass(entityClass)
                .daoClass(getClass())
                .opType("execute")
                .build();
    }

    private T generateEntity() {
        T result = null;
        switch (mode) {
            case READ:
                result = getter.get();
                if (result == null) {
                    throw new RuntimeException("Entity doesn't exist");
                }
                break;
            case INSERT:
                result = saver.apply(entity);
                break;
            default:
                break;

        }
        return result;
    }
}