package io.appform.dropwizard.sharding;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.appform.dropwizard.sharding.dao.LookupDao;
import io.appform.dropwizard.sharding.dao.testdata.entities.ScrollTestEntity;
import io.appform.dropwizard.sharding.scroll.ScrollPointer;
import io.appform.dropwizard.sharding.scroll.ScrollResult;
import io.appform.dropwizard.sharding.sharding.BalancedShardManager;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import io.appform.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.DetachedCriteria;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

/**
 * Tests out functionality for {@link LookupDao#scrollDown(DetachedCriteria, ScrollPointer, int, String)}
 */
@Slf4j
public class ScrollTest {
    private final List<SessionFactory> sessionFactories = Lists.newArrayList();

    private LookupDao<ScrollTestEntity> lookupDao;

    @Before
    public void before() {
        for (int i = 0; i < 2; i++) {
            sessionFactories.add(buildSessionFactory(String.format("db_%d", i)));
        }
        final ShardManager shardManager = new BalancedShardManager(sessionFactories.size());
        final ShardCalculator<String> shardCalculator = new ShardCalculator<>(shardManager,
                                                                              new ConsistentHashBucketIdExtractor<>(
                                                                                      shardManager));
        lookupDao = new LookupDao<>(sessionFactories, ScrollTestEntity.class, shardCalculator);
    }

    @Test
    public void testScrollUp() {
        val numEntities = 400;
        val pageSize = 10;
        val ids = new HashSet<Integer>();
        val entities = new HashSet<Integer>();

        populateEntities(1, numEntities, ids);

        var result = (ScrollResult<ScrollTestEntity>)null;
        var pointer = (ScrollPointer) null;

        //Now scroll up to read everything
        do {
            result = lookupDao.scrollUp(DetachedCriteria.forClass(ScrollTestEntity.class),
                                     pointer,
                                     pageSize,
                                     "id");
            addValuesToSet(entities, result);
            pointer = result.getPointer();
            log.info("Received {} entities", result.getResult().stream().map(ScrollTestEntity::getId).collect(Collectors.toList()));
        } while (result.getResult().size() != 0);
        assertTrue( "There are " + Sets.difference(ids, entities) + " ids missing in scroll", entities.containsAll(ids));
    }

    @Test
    public void testScrollSorted() {
        val numEntities = 400;
        val ids = new HashSet<Integer>();
        val entities = new HashSet<Integer>();

        populateEntities(1, numEntities, ids);
        var result = (ScrollResult<ScrollTestEntity>)null;
        var pointer = (ScrollPointer)null;
        do {
            result = lookupDao.scrollDown(DetachedCriteria.forClass(ScrollTestEntity.class),
                                     pointer,
                                     10,
                                     "id");
            addValuesToSet(entities, result);
            pointer = result.getPointer();
            log.info("Received {} entities", result.getResult().stream().map(ScrollTestEntity::getId).collect(Collectors.toList()));
        } while (result.getResult().size() != 0);
        assertTrue( "There are " + Sets.difference(ids, entities).size() + " ids missing in scroll", entities.containsAll(ids));
        populateEntities(numEntities + 1, 2 * numEntities, ids);

        /*Pointer does not need to be reset here. This is because sort order is on the auto increment id field.
        If it is not such a field, the results of scroll would be wrong for obvious reasons*/

        do {
            result = lookupDao.scrollDown(DetachedCriteria.forClass(ScrollTestEntity.class),
                                     pointer,
                                     10,
                                     "id");
            addValuesToSet(entities, result);
            pointer = result.getPointer();
            log.info("Received {} entities", result.getResult().size());
        } while (result.getResult().size() != 0);
        assertTrue( "There are " + Sets.difference(ids, entities).size() + " ids missing in scroll", entities.containsAll(ids));
    }

    private static void addValuesToSet(HashSet<Integer> entities, ScrollResult<ScrollTestEntity> result) {
        result.getResult().stream().map(ScrollTestEntity::getValue).forEach(entities::add);
    }

    private SessionFactory buildSessionFactory(String dbName) {
        Configuration configuration = new Configuration();
        configuration.setProperty("hibernate.dialect",
                                  "org.hibernate.dialect.H2Dialect");
        configuration.setProperty("hibernate.connection.driver_class",
                                  "org.h2.Driver");
        configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:" + dbName);
        configuration.setProperty("hibernate.hbm2ddl.auto", "create");
        configuration.setProperty("hibernate.current_session_context_class", "managed");
        configuration.setProperty("hibernate.show_sql", "true");
        configuration.setProperty("hibernate.format_sql", "true");
        configuration.addAnnotatedClass(ScrollTestEntity.class);

        StandardServiceRegistry serviceRegistry
                = new StandardServiceRegistryBuilder().applySettings(
                        configuration.getProperties())
                .build();
        return configuration.buildSessionFactory(serviceRegistry);
    }

    private void populateEntities(int start, int end, HashSet<Integer> ids) {
        ids.addAll(IntStream.rangeClosed(start, end)
                           .mapToObj(i -> {
                               try {
                                   return lookupDao.save(new ScrollTestEntity(0, UUID.randomUUID().toString(), i))
                                           .orElse(null)
                                           ;
                               }
                               catch (Exception e) {
                                   throw new RuntimeException(e);
                               }
                           })
                           .filter(Objects::nonNull)
                           .map(ScrollTestEntity::getValue)
                           .collect(Collectors.toSet()));
    }
}
