package io.appform.dropwizard.sharding.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.appform.dropwizard.sharding.ShardInfoProvider;
import io.appform.dropwizard.sharding.config.ShardingBundleOptions;
import io.appform.dropwizard.sharding.dao.interceptors.TimerObserver;
import io.appform.dropwizard.sharding.dao.listeners.LoggingListener;
import io.appform.dropwizard.sharding.observers.internal.ListenerTriggeringObserver;
import io.appform.dropwizard.sharding.sharding.BalancedShardManager;
import io.appform.dropwizard.sharding.sharding.ShardManager;
import io.appform.dropwizard.sharding.sharding.impl.ConsistentHashBucketIdExtractor;
import io.appform.dropwizard.sharding.utils.ShardCalculator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.val;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Restrictions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.util.ArrayList;
import java.util.List;

public class RelationalReadOnlyLockedContextTest {

    private List<SessionFactory> sessionFactories = Lists.newArrayList();
    private RelationalDao<Department> departmentRelationalDao;
    private RelationalDao<Ceo> ceoRelationalDao;
    private RelationalDao<Company> companyRelationalDao;

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
        configuration.addAnnotatedClass(Company.class);
        configuration.addAnnotatedClass(Department.class);
        configuration.addAnnotatedClass(Ceo.class);
        StandardServiceRegistry serviceRegistry
                = new StandardServiceRegistryBuilder().applySettings(
                        configuration.getProperties())
                .build();
        return configuration.buildSessionFactory(serviceRegistry);
    }

    @BeforeEach
    public void before() {
        for (int i = 0; i < 2; i++) {
            sessionFactories.add(buildSessionFactory(String.format("db_%d", i)));
        }
        final ShardManager shardManager = new BalancedShardManager(sessionFactories.size());
        final ShardCalculator<String> shardCalculator = new ShardCalculator<>(shardManager,
                new ConsistentHashBucketIdExtractor<>(
                        shardManager));
        final ShardingBundleOptions shardingOptions = new ShardingBundleOptions();
        final ShardInfoProvider shardInfoProvider = new ShardInfoProvider("default");
        val observer = new TimerObserver(new ListenerTriggeringObserver().addListener(new LoggingListener()));

        companyRelationalDao = new RelationalDao<>(sessionFactories, Company.class, shardCalculator, shardingOptions,
                shardInfoProvider, observer);
        departmentRelationalDao = new RelationalDao<>(sessionFactories, Department.class, shardCalculator, shardingOptions,
                shardInfoProvider, observer);
        ceoRelationalDao = new RelationalDao<>(sessionFactories, Ceo.class, shardCalculator, shardingOptions,
                shardInfoProvider, observer);
    }

    @AfterEach
    public void after() {
        sessionFactories.forEach(SessionFactory::close);
    }

    @Test
    @SneakyThrows
    public void testRelationalDaoReadOnlyContext() {
        Company company1 = Company.builder()
                .companyUsageId("CMPID1")
                .companyId(1l)
                .name("COMP1")
                .build();
        Department eng = Department.builder()
                .name("ENGINEERING")
                .companyExtId(company1.companyUsageId)
                .build();
        Department fin = Department.builder()
                .name("FINANCE")
                .companyExtId(company1.companyUsageId)
                .build();
        Ceo ceo1 = Ceo.builder()
                .companyExtId(company1.companyUsageId)
                .name("KING")
                .build();


        Company company2 = Company.builder()
                .companyId(2l)
                .companyUsageId("CMPID2")
                .name("COMP2")
                .build();
        Department hr = Department.builder()
                .name("HR")
                .companyExtId(company2.companyUsageId)
                .build();
        Ceo ceo2 = Ceo.builder()
                .companyExtId(company2.companyUsageId)
                .name("KING-2")
                .build();


        String parentKey = "PARENT_KEY";
        val lockedContext1 = companyRelationalDao.saveAndGetExecutor(parentKey, company1);
        lockedContext1.save(departmentRelationalDao, eng1 -> eng);
        lockedContext1.save(departmentRelationalDao, fin1 -> fin);
        lockedContext1.save(ceoRelationalDao, ceo -> ceo1);
        lockedContext1.execute();

        val lockedContext2 = companyRelationalDao.saveAndGetExecutor(parentKey, company2);
        lockedContext2.save(departmentRelationalDao, hr1 -> hr);
        lockedContext2.save(ceoRelationalDao, ceo -> ceo2);
        lockedContext2.execute();


        // USE CASE 1 : Using Association Key Annotations
        val criteria = DetachedCriteria.forClass(Company.class)
                .add(Restrictions.in("companyId", Sets.newHashSet(company1.getCompanyId(), company2.getCompanyId())));
        val assosicationSpec = Lists.newArrayList(
                RelationalDao.QueryAssociationSpec.builder().childMappingKey("companyExtId").parentMappingKey("companyUsageId").build()
        );

        val dataList = companyRelationalDao.readOnlyExecutor(parentKey, criteria, 0, 4)
                .readAugmentParent(departmentRelationalDao, null, assosicationSpec, 0, Integer.MAX_VALUE, Company::setDepartments)
                .readAugmentParent(ceoRelationalDao, null, assosicationSpec, 0, Integer.MAX_VALUE, (parent, childList) -> {
                    parent.setCeo(childList.stream().findAny().orElse(null));
                })
                .execute()
                .orElse(new ArrayList<>());
        System.out.println(new ObjectMapper().writeValueAsString(dataList));



        // USE CASE 2 : Not Using Association Key Annotations
        val companyToRetrieve = company1.companyUsageId;
        val companyCriteria = DetachedCriteria.forClass(Company.class)
                .add(Restrictions.eq("companyUsageId", companyToRetrieve));

        val deptCriteria = DetachedCriteria.forClass(Department.class)
                .add(Restrictions.eq("companyExtId", companyToRetrieve));

        val dataList2 = companyRelationalDao.readOnlyExecutor(parentKey, companyCriteria, 0, 4)
                .readAugmentParent(departmentRelationalDao, deptCriteria, null, 0, Integer.MAX_VALUE, Company::setDepartments)
                .execute()
                .orElse(new ArrayList<>());
        System.out.println(new ObjectMapper().writeValueAsString(dataList2));




    }


    @Entity
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Table(name = "company")
    public static class Company {
        @Id
        @Column(name = "company_id", nullable = false, unique = true)
        private long companyId;

        @Column(name = "companyUsageId", nullable = false)
        private String companyUsageId;

        @Column(name = "name", nullable = false)
        private String name;

        @Transient
        private List<Department> departments;

        @Transient
        private Ceo ceo;

    }

    @Entity
    @Table(name = "departments")
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Builder
    public static class Department {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "id")
        private long id;

        @Column(name = "name")
        private String name;

        @Column(name = "company_ext_id", nullable = false)
        private String companyExtId;

    }

    @Entity
    @Table(name = "ceo")
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @Builder
    public static class Ceo {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Column(name = "id")
        private long id;

        @Column(name = "name")
        private String name;

        @Column(name = "company_ext_id", nullable = false)
        private String companyExtId;

    }
}
