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

package io.appform.dropwizard.sharding.dao;

import com.google.common.base.Preconditions;
import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.dao.operations.Run;
import io.appform.dropwizard.sharding.dao.operations.Save;
import io.appform.dropwizard.sharding.dao.operations.SelectAndUpdate;
import io.appform.dropwizard.sharding.dao.operations.UpdateByQuery;
import io.appform.dropwizard.sharding.dao.operations.Count;
import io.appform.dropwizard.sharding.dao.operations.relationaldao.CreateOrUpdate;
import io.appform.dropwizard.sharding.dao.operations.relationaldao.CreateOrUpdateInLockedContext;
import io.appform.dropwizard.sharding.dao.operations.Get;
import io.appform.dropwizard.sharding.dao.operations.GetAndUpdate;
import io.appform.dropwizard.sharding.dao.operations.SaveAll;
import io.appform.dropwizard.sharding.dao.operations.ScrollParam;
import io.appform.dropwizard.sharding.dao.operations.Select;
import io.appform.dropwizard.sharding.dao.operations.SelectParam;
import io.appform.dropwizard.sharding.dao.operations.Update;
import io.appform.dropwizard.sharding.dao.operations.UpdateWithScroll;
import io.appform.dropwizard.sharding.execution.TransactionExecutor;
import io.appform.dropwizard.sharding.observers.TransactionObserver;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import io.dropwizard.hibernate.AbstractDAO;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hibernate.*;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.query.Query;

import javax.persistence.Id;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A dao used to work with entities related to a parent shard. The parent may or maynot be physically present.
 * A murmur 128 hash of the string parent key is used to route the save and retrieve calls from the proper shard.
 */
@Slf4j
public class RelationalDao<T> implements ShardedDao<T> {

    private final class RelationalDaoPriv extends AbstractDAO<T> {

        private final SessionFactory sessionFactory;

        /**
         * Creates a new DAO with a given session provider.
         *
         * @param sessionFactory a session provider
         */
        public RelationalDaoPriv(SessionFactory sessionFactory) {
            super(sessionFactory);
            this.sessionFactory = sessionFactory;
        }

        T get(Object lookupKey) {
            return uniqueResult(currentSession()
                                        .createCriteria(entityClass)
                                        .add(Restrictions.eq(keyField.getName(), lookupKey))
                                        .setLockMode(LockMode.READ));
        }

        DetachedCriteria getDetachedCriteria(Object lookupKey) {
            return DetachedCriteria.forClass(entityClass).add(Restrictions.eq(keyField.getName(), lookupKey))
                .setLockMode(LockMode.READ);
        }

        T get(DetachedCriteria criteria) {
            return uniqueResult(criteria.getExecutableCriteria(currentSession()));
        }

        T getLockedForWrite(DetachedCriteria criteria) {
            return uniqueResult(criteria.getExecutableCriteria(currentSession())
                                        .setLockMode(LockMode.UPGRADE_NOWAIT));
        }

        T save(T entity) {
            return persist(entity);
        }

        boolean saveAll(Collection<T> entities) {
            for (T entity : entities) {
                persist(entity);
            }
            return true;
        }

        void update(T oldEntity, T entity) {
            currentSession().evict(oldEntity); //Detach .. otherwise update is a no-op
            currentSession().update(entity);
        }

        List<T> select(SelectParam selectParam) {
            val criteria = selectParam.getCriteria().getExecutableCriteria(currentSession());
            criteria.setFirstResult(selectParam.getStart());
            criteria.setMaxResults(selectParam.getNumRows());
            return list(criteria);
        }

        ScrollableResults scroll(ScrollParam scrollDetails) {
            final Criteria criteria = scrollDetails.getCriteria().getExecutableCriteria(currentSession());
            return criteria.scroll(ScrollMode.FORWARD_ONLY);
        }

        long count(DetachedCriteria criteria) {
            return (long) criteria.getExecutableCriteria(currentSession())
                    .setProjection(Projections.rowCount())
                    .uniqueResult();
        }

        public int update(final UpdateOperationMeta updateOperationMeta) {
            Query query = currentSession().createNamedQuery(updateOperationMeta.getQueryName());
            updateOperationMeta.getParams().forEach(query::setParameter);
            return query.executeUpdate();
        }

    }

    private List<RelationalDaoPriv> daos;
    private final Class<T> entityClass;
    @Getter
    private final ShardCalculator<String> shardCalculator;
    private final Field keyField;

    private final TransactionExecutor transactionExecutor;
    private final ShardInfoProvider shardInfoProvider;
    private final TransactionObserver observer;

    /**
     * Create a relational DAO.
     *
     * @param sessionFactories List of session factories. One for each shard.
     * @param entityClass      The class for which the dao will be used.
     * @param shardCalculator
     */
    public RelationalDao(
            List<SessionFactory> sessionFactories,
            Class<T> entityClass,
            ShardCalculator<String> shardCalculator,
            final ShardInfoProvider shardInfoProvider,
            final TransactionObserver observer) {
        this.shardCalculator = shardCalculator;
        this.daos = sessionFactories.stream().map(RelationalDaoPriv::new).collect(Collectors.toList());
        this.entityClass = entityClass;
        this.shardInfoProvider = shardInfoProvider;
        this.observer = observer;
        this.transactionExecutor = new TransactionExecutor(shardInfoProvider, getClass(), entityClass, observer);

        Field[] fields = FieldUtils.getFieldsWithAnnotation(entityClass, Id.class);
        Preconditions.checkArgument(fields.length != 0, "A field needs to be designated as @Id");
        Preconditions.checkArgument(fields.length == 1, "Only one field can be designated as @Id");
        keyField = fields[0];
        if (!keyField.isAccessible()) {
            try {
                keyField.setAccessible(true);
            }
            catch (SecurityException e) {
                log.error("Error making key field accessible please use a public method and mark that as @Id", e);
                throw new IllegalArgumentException("Invalid class, DAO cannot be created.", e);
            }
        }
    }


    public Optional<T> get(String parentKey, Object key) throws Exception {
        return Optional.ofNullable(get(parentKey, key, t -> t));
    }

    public <U> U get(String parentKey, Object key, Function<T, U> function) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        val opContext = Get.<T, U>builder()
            .getter(dao::get)
            .criteria(dao.getDetachedCriteria(key))
            .afterGet(function).build();
        return transactionExecutor.execute(dao.sessionFactory, true, "get", opContext, shardId);
    }

    public Optional<T> save(String parentKey, T entity) throws Exception {
        return Optional.ofNullable(save(parentKey, entity, t -> t));
    }

    public <U> U save(String parentKey, T entity, Function<T, U> handler) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        val opContext = Save.<T, U>builder()
            .saver(dao::save).entity(entity).afterSave(handler).build();
        return transactionExecutor.execute(dao.sessionFactory, false, "save", opContext,
                                           shardId);
    }

    public boolean saveAll(String parentKey, Collection<T> entities) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        val opContext = SaveAll.<T>builder().saver(dao::saveAll).entities(entities).build();
        return transactionExecutor.execute(dao.sessionFactory, false, "saveAll", opContext, shardId);
    }

    public Optional<T> createOrUpdate(
        final String parentKey,
        final DetachedCriteria selectionCriteria,
        final UnaryOperator<T> updater,
        final Supplier<T> entityGenerator) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        val opContext = CreateOrUpdate.<T>builder()
            .criteria(selectionCriteria)
            .getLockedForWrite(dao::getLockedForWrite)
            .entityGenerator(entityGenerator)
            .saver(dao::save)
            .mutator(updater)
            .updater(dao::update)
            .getter(dao::get)
            .build();
        return Optional.of(transactionExecutor.execute(
            dao.sessionFactory,
            false,
            "createOrUpdate",
            opContext,
            shardId));
    }

    public <U> void save(LockedContext<U> context, T entity) {
        RelationalDaoPriv dao = daos.get(context.getShardId());
        val opContext = Save.<T, T>builder().entity(entity).saver(dao::save).build();
        transactionExecutor.execute(context.getSessionFactory(), false, "save", opContext, context.getShardId(), false);
    }

    <U> void save(LockedContext<U> context, T entity, Function<T, T> handler) {
        RelationalDaoPriv dao = daos.get(context.getShardId());
        val opContext = Save.<T, T>builder().entity(entity).saver(dao::save).afterSave(handler).build();

        transactionExecutor.execute(context.getSessionFactory(), false, "save", opContext, context.getShardId(), false);
    }

    <U> boolean update(LockedContext<U> context, Object id, Function<T, T> updater) {
        RelationalDaoPriv dao = daos.get(context.getShardId());
        return update(context.getShardId(), context.getSessionFactory(), dao, id, updater, false);
    }

    <U> boolean update(
            LockedContext<U> context,
            DetachedCriteria criteria,
            Function<T, T> updater,
            BooleanSupplier updateNext) {
        final RelationalDaoPriv dao = daos.get(context.getShardId());
        val opContext = UpdateWithScroll.<T>builder()
            .scroller(dao::scroll)
            .scrollParam(ScrollParam.builder()
                .criteria(criteria)
                .build())
            .mutator(updater)
            .updater(dao::update)
            .updateNext(updateNext)
            .build();

        try {
            return transactionExecutor.execute(context.getSessionFactory(),
                true,
                "update",
                opContext,
                context.getShardId(), false);
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity with scroll: " + criteria, e);
        }
    }

    <U> List<T> select(
            LookupDao.ReadOnlyContext<U> context,
            DetachedCriteria criteria,
            int first,
            int numResults) throws Exception {
        final RelationalDaoPriv dao = daos.get(context.getShardId());
        val opContext = Select.<T, List<T>>builder()
            .getter(dao::select)
            .criteria(criteria)
            .start(first)
            .numRows(numResults).build();
        return transactionExecutor.execute(context.getSessionFactory(), true, "select", opContext, context.getShardId());
    }

    public boolean update(String parentKey, Object id, Function<T, T> updater) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        return update(shardId, dao.sessionFactory, dao, id, updater, true);
    }

    public <U> U runInSession(String id, Function<Session, U> handler) {
        int shardId = shardCalculator.shardId(id);
        RelationalDaoPriv dao = daos.get(shardId);
        val opContext = Run.<U>builder().handler(handler).build();
        return transactionExecutor.
            execute(dao.sessionFactory, true, "runInSession", opContext, shardId);
    }

    private boolean update(
            int shardId,
            SessionFactory daoSessionFactory,
            RelationalDaoPriv dao,
            Object id,
            Function<T, T> updater,
            boolean completeTransaction) {

        val opContext = GetAndUpdate.<T>builder()
            .criteria(dao.getDetachedCriteria(id))
            .getter(dao::get)
            .mutator(updater)
            .updater(dao::update).build();

        try {
            return transactionExecutor.execute(daoSessionFactory, true, "update",
                                                                   opContext, shardId, completeTransaction);
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity: " + id, e);
        }
    }

    public boolean update(String parentKey, DetachedCriteria criteria, Function<T, T> updater) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        val  selectParam = SelectParam.builder()
            .criteria(criteria)
            .start(0)
            .numRows(1)
            .build();
        val opContext = SelectAndUpdate.<T>builder()
            .selectParam(selectParam)
            .selector(dao::select)
            .mutator(updater)
            .updater(dao::update).build();
        try {
            return transactionExecutor.execute(dao.sessionFactory,
                true,
                "update",
                opContext,
                shardId);
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity with criteria: " + criteria, e);
        }
    }


    public int updateUsingQuery(String parentKey, UpdateOperationMeta updateOperationMeta) {
        int shardId = shardCalculator.shardId(parentKey);
        val dao = daos.get(shardId);
        val opContext = UpdateByQuery.builder()
            .updater(dao::update).updateOperationMeta(updateOperationMeta).build();
        return transactionExecutor.execute(dao.sessionFactory, false, "updateUsingQuery", opContext, shardId);
    }

    public <U> int updateUsingQuery(LockedContext<U> lockedContext, UpdateOperationMeta updateOperationMeta) {
        val dao = daos.get(lockedContext.getShardId());
        val opContext = UpdateByQuery.builder()
            .updater(dao::update).updateOperationMeta(updateOperationMeta).build();
        return transactionExecutor.execute(lockedContext.getSessionFactory(),
                                           false,
                                           "updateUsingQuery",
                                           opContext,
                                           lockedContext.getShardId(),false);
    }

    public LockedContext<T> lockAndGetExecutor(String parentKey, DetachedCriteria criteria) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        return new LockedContext<T>(shardId, dao.sessionFactory, () -> dao.getLockedForWrite(criteria),
                entityClass, shardInfoProvider, observer);
    }

    public LockedContext<T> saveAndGetExecutor(String parentKey, T entity) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        return new LockedContext<T>(shardId, dao.sessionFactory, dao::save, entity,
                entityClass, shardInfoProvider, observer);
    }

    <U> boolean createOrUpdate(
        LockedContext<U> context,
        DetachedCriteria criteria,
        UnaryOperator<T> updater,
        U parent,
        Function<U, T> entityGenerator) {
        final RelationalDaoPriv dao = daos.get(context.getShardId());
        val opContext = CreateOrUpdateInLockedContext.<T, U>builder()
            .lockedEntity(parent)
            .getter(dao::get)
            .criteria(criteria)
            .entityGenerator(entityGenerator)
            .saver(dao::save)
            .mutator(updater)
            .updater(dao::update)
            .build();

        try {
            return transactionExecutor.execute(context.getSessionFactory(),
                true,
                "createOrUpdate",
                opContext,
                context.getShardId(), false);
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity with criteria: " + criteria, e);
        }
    }

    public boolean updateAll(
            String parentKey,
            int start,
            int numRows,
            DetachedCriteria criteria,
            Function<T, T> updater) {
        int shardId = shardCalculator.shardId(parentKey);
        RelationalDaoPriv dao = daos.get(shardId);
        try {
            val opContext = Update.<T>builder()
                .criteria(criteria)
                .start(start)
                .numRows(numRows)
                .getter(dao::select)
                .mutator(updater)
                .updater(dao::update).build();
            return transactionExecutor.<Boolean>execute(dao.sessionFactory, true, "updateAll",
                                                                                  opContext,
                                                                                  shardId);
        }
        catch (Exception e) {
            throw new RuntimeException("Error updating entity with criteria: " + criteria, e);
        }
    }

    public List<T> select(String parentKey, DetachedCriteria criteria, int first, int numResults) throws
                                                                                                  Exception {
        return select(parentKey, criteria, first, numResults, t -> t);
    }

    public <U> U select(
            String parentKey,
            DetachedCriteria criteria,
            int first,
            int numResults,
            Function<List<T>, U> handler) throws Exception {

            int shardId = shardCalculator.shardId(parentKey);
            RelationalDaoPriv dao = daos.get(shardId);
        val opContext = Select.<T, U>builder()
            .getter(dao::select)
            .criteria(criteria)
            .start(first)
            .numRows(numResults)
            .afterSelect(handler).build();

        return transactionExecutor.execute(dao.sessionFactory,
                                               true,
                                             "select",
                                             opContext,
                                               shardId);
        }

        public long count (String parentKey, DetachedCriteria criteria){
            int shardId = shardCalculator.shardId(parentKey);
            RelationalDaoPriv dao = daos.get(shardId);
            val opContext = Count.builder().counter(dao::count).criteria(criteria).build();
            return transactionExecutor.<Long>execute(dao.sessionFactory,
                                                                       true,
                "count", opContext,
                                                                       shardId);
        }


        public boolean exists (String parentKey, Object key){
            int shardId = shardCalculator.shardId(parentKey);
            RelationalDaoPriv dao = daos.get(shardId);
            val opContext = Get.<T, T>builder()
                .criteria(dao.getDetachedCriteria(key))
                .getter(dao::get).build();
            Optional<T>
                result = Optional.ofNullable(transactionExecutor.execute(dao.sessionFactory,
                                                                                  true,
                                                                                  "exists",
                                                                                  opContext,
                                                                                  shardId));
            return result.isPresent();
        }

        /**
         * Queries using the specified criteria across all shards and returns the counts of rows satisfying
         * the criteria.
         * <b>Note:</b> This method runs the query serially and it's usage is not recommended.
         *
         * @param criteria The select criteria
         * @return List of counts in each shard
         */
        public List<Long> countScatterGather (DetachedCriteria criteria){

            return IntStream.range(0, daos.size())
                    .mapToObj(shardId -> {
                        val dao = daos.get(shardId);
                        try {
                            val opContext = Count.builder()
                                .counter(dao::count).criteria(criteria).build();
                            return transactionExecutor.execute(dao.sessionFactory, true,
                                "countScatterGather", opContext, shardId);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList());
        }

        public List<T> scatterGather (DetachedCriteria criteria,int start, int numRows){
            return IntStream.range(0, daos.size())
                    .mapToObj(shardId -> {
                        val dao = daos.get(shardId);
                        try {
                            val opContext = Select.<T, List<T>>builder()
                                .getter(dao::select)
                                .criteria(criteria)
                                .start(start)
                                .numRows(numRows).build();
                            return transactionExecutor.execute(dao.sessionFactory,
                                                               true,
                                                               "scatterGather",
                                                               opContext,
                                                               shardId);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).flatMap(Collection::stream).collect(Collectors.toList());
        }

        protected Field getKeyField () {
            return this.keyField;
        }

    }
