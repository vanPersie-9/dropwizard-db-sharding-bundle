package io.appform.dropwizard.sharding.dao;

import com.google.common.collect.ImmutableList;
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
import org.hibernate.annotations.Cascade;
import org.hibernate.annotations.CascadeType;
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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import java.util.List;
import java.util.stream.Collectors;

public class RelationalReadOnlyLockedContextTest {

    private List<SessionFactory> sessionFactories = Lists.newArrayList();
    private RelationalDao<Department> departmentRelationalDao;
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
                .company(company1)
                .companyExtId(company1.companyUsageId)
                .build();
        Department fin = Department.builder()
                .name("FINANCE")
                .company(company1)
                .companyExtId(company1.companyUsageId)
                .build();
        company1.setDepartments(ImmutableList.of(eng, fin));

        Company company2 = Company.builder()
                .companyId(2l)
                .companyUsageId("CMPID2")
                .name("COMP2")
                .build();
        Department hr = Department.builder()
                .name("HR")
                .company(company2)
                .companyExtId(company2.companyUsageId)
                .build();
        company2.setDepartments(ImmutableList.of(hr));


        String parentKey = "PARENT_KEY";
        companyRelationalDao.save(parentKey, company1);
        companyRelationalDao.save(parentKey, company2);


        val criteria = DetachedCriteria.forClass(Company.class)
                .add(Restrictions.in("companyId", Sets.newHashSet(company1.getCompanyId(), company2.getCompanyId())));

        val deptCriteria = DetachedCriteria.forClass(Department.class)
                .add(Restrictions.in("companyExtId", Sets.newHashSet(company1.companyUsageId, company2.companyUsageId)));

        // Use case 1 : GET PARENT BY ID
        companyRelationalDao.readOnlyExecutor(parentKey, company1.companyId)
                .readAugmentParent(departmentRelationalDao, deptCriteria, 0, Integer.MAX_VALUE, (parent, childrenList) -> {
                    System.out.println("-------------");
                    List<Department> dataList = childrenList.stream().filter(children -> children.getCompanyExtId().equals(parent.companyUsageId)).collect(Collectors.toList());
                    System.out.println(parent.getName());
                    System.out.println(dataList.stream().map(Department::getName).collect(Collectors.joining("-")));
                    System.out.println("-------------");
                })
                .execute();

        // Use case 2 : GET PARENT BY CRITERIA with readAugmentParent() -> Not recommended
        // But this will return multiple parent & hence multiple time same query of child will be executed
        companyRelationalDao.readOnlyExecutor(parentKey, criteria, 0, Integer.MAX_VALUE)
                .readAugmentParent(departmentRelationalDao, deptCriteria, 0, Integer.MAX_VALUE, (parent, childrenList) -> {
                    System.out.println("-------------");
                    List<Department> dataList = childrenList.stream().filter(children -> children.getCompanyExtId().equals(parent.companyUsageId)).collect(Collectors.toList());
                    System.out.println(parent.getName());
                    System.out.println(dataList.stream().map(Department::getName).collect(Collectors.joining("-")));
                    System.out.println("-------------");
                })
                .execute();

        // Use case 2 : GET PARENT BY CRITERIA with readAugmentMultiParent()
        // This invokes parent & child query only once, consumer need to handler filtering & setting logic
        companyRelationalDao.readOnlyExecutor(parentKey, criteria, 0, 4)
                .readAugmentMultiParent(departmentRelationalDao, deptCriteria, 0, Integer.MAX_VALUE, (parentList, childrenList) -> {
                    parentList.stream().forEach(parent -> {
                        System.out.println("-------------");
                        List<Department> dataList = childrenList.stream().filter(children -> children.getCompanyExtId().equals(parent.companyUsageId)).collect(Collectors.toList());
                        System.out.println(parent.getName());
                        System.out.println(dataList.stream().map(Department::getName).collect(Collectors.joining("-")));
                        System.out.println("-------------");
                    });
                })
                .execute();

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

        @ManyToOne
        @JoinColumn(name = "company_id")
        private Company company;

        @Column(name = "company_ext_id", nullable = false)
        private String companyExtId;

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

        @OneToMany(mappedBy = "company")
        @Cascade(CascadeType.ALL)
        private List<Department> departments;


    }
}
