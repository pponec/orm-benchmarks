package org.benchmark.hibernate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.Stopwatch;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.cfg.Configuration;
import org.benchmark.common.OrmBenchmark;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Main benchmark class for Hibernate.
 * Uses entity/session APIs (including StatelessSession for bulk work), which is idiomatic Hibernate usage.
 */
public class HibernateBenchmark implements OrmBenchmark {

    /** City entity mapping */
    @Entity(name = "HibCity")
    @Table(name = "city")
    @Getter
    @Setter
    public static class City {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        private String name;
        @Column(name = "country_code") private String countryCode;
        private BigDecimal latitude;
        private BigDecimal longitude;
        @Column(name = "created_at") private LocalDateTime createdAt;
        @Column(name = "updated_at") private LocalDateTime updatedAt;
        @Column(name = "created_by") private String createdBy;
        @Column(name = "updated_by") private String updatedBy;
    }

    /** Employee entity mapping */
    @Entity(name = "HibEmployee")
    @Table(name = "employee")
    @DynamicUpdate
    @Getter
    @Setter
    public static class Employee {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;
        private String name;

        @ManyToOne(fetch = FetchType.LAZY)
        @JoinColumn(name = "superior_id")
        private Employee superior;

        @ManyToOne(fetch = FetchType.LAZY)
        @JoinColumn(name = "city_id", nullable = false)
        private City city;

        @Column(name = "contract_day") private LocalDate contractDay;
        @Column(name = "is_active") private Boolean isActive;
        private String email;
        private String phone;
        private BigDecimal salary;
        private String department;
        @Column(name = "created_at") private LocalDateTime createdAt;
        @Column(name = "updated_at") private LocalDateTime updatedAt;
        @Column(name = "created_by") private String createdBy;
        @Column(name = "updated_by") private String updatedBy;
    }

    /** Data Access Object for entities */
    public static class Dao {
        private final Session session;

        public Dao(Session session) {
            this.session = session;
        }

        /** Persists a new entity to the database */
        public void insert(Object entity) {
            session.persist(entity);
        }

        /** Retrieves a City from the database */
        public City getCity(Long id) {
            var result = session.find(City.class, id);
            if (result == null) {
                throw new IllegalStateException("City with ID " + id + " not found. Ensure 'common' module is recompiled!");
            }
            return result;
        }
    }

    /** Service layer managing transactions */
    public static class Service implements AutoCloseable {
        private final SessionFactory sessionFactory;

        public Service() {
            var configuration = new Configuration();
            configuration.addAnnotatedClass(City.class);
            configuration.addAnnotatedClass(Employee.class);
            configuration.setProperty("hibernate.connection.provider_class", "org.hibernate.engine.jdbc.connections.internal.DriverManagerConnectionProviderImpl");
            configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1");
            configuration.setProperty("hibernate.connection.username", "sa");
            configuration.setProperty("hibernate.connection.password", "");
            configuration.setProperty("hibernate.dialect", "org.hibernate.dialect.H2Dialect");
            configuration.setProperty("hibernate.jdbc.batch_size", "50");
            configuration.setProperty("hibernate.order_inserts", "true");
            configuration.setProperty("hibernate.order_updates", "true");
            this.sessionFactory = configuration.buildSessionFactory();
        }

        /** Executes the given action inside a managed database transaction */
        public void executeInTransaction(Consumer<Dao> action) {
            try (var connection = DatabaseUtils.getConnection();
                 var session = sessionFactory.withOptions().connection(connection).openSession()) {
                var transaction = session.beginTransaction();
                try {
                    action.accept(new Dao(session));
                    transaction.commit();
                } catch (Exception e) {
                    transaction.rollback();
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /** Executes the given action inside a managed Stateless transaction */
        public void executeInStatelessTransaction(Consumer<StatelessSession> action) {
            try (var connection = DatabaseUtils.getConnection();
                 var session = sessionFactory.withStatelessOptions().connection(connection).openStatelessSession()) {
                var transaction = session.beginTransaction();
                try {
                    action.accept(session);
                    transaction.commit();
                } catch (Exception e) {
                    transaction.rollback();
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /** Executes the given action in a read-only transaction */
        public void executeReadOnly(Consumer<Session> action) {
            try (var connection = DatabaseUtils.getConnection();
                 var session = sessionFactory.withOptions().connection(connection).openSession()) {
                session.setDefaultReadOnly(true);
                var transaction = session.beginTransaction();
                try {
                    action.accept(session);
                    transaction.commit();
                } catch (Exception e) {
                    transaction.rollback();
                    throw e;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            sessionFactory.close();
        }
    }

    private final Service service;

    public HibernateBenchmark() {
        this.service = new Service();
    }

    /** Executes a single row insert test */
    public int testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    dao.insert(employee);
                }
            });
        });
        return stopwatch.getIterations();
    }

    /** Executes a batch insert test using StatelessSession for maximum performance */
    @Override
    public int testBatchInsert(Stopwatch stopwatch) {
        service.executeInStatelessTransaction(session -> {
            var city = session.get(City.class, 1L);
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    batch.add(createRandomEmployee(city));
                    if (i % 50 == 0) {
                        session.insertMultiple(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    session.insertMultiple(batch);
                }
            });
        });
        return stopwatch.getIterations();
    }

    /** Executes updates on selected columns */
    @Override
    public int testSpecificUpdate(Stopwatch stopwatch) {
        var updatedCount = new AtomicReference<>(0);
        service.executeInStatelessTransaction(session -> {
            var employees = session.createQuery("from HibEmployee", Employee.class).list();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var employee : employees) {
                    employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                    employee.setUpdatedAt(LocalDateTime.now());
                    batch.add(employee);

                    if (batch.size() == 50) {
                        session.updateMultiple(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    session.updateMultiple(batch);
                }
            });
        });
        return updatedCount.get();
    }

    /** Executes updates on randomly modified columns */
    @Override
    public int testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        var updatedCount = new AtomicReference<>(0);
        service.executeInStatelessTransaction(session -> {
            var employees = session.createQuery("from HibEmployee", Employee.class).list();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var employee : employees) {
                    if (random.nextBoolean()) {
                        employee.setIsActive(!employee.getIsActive());
                    } else {
                        employee.setDepartment("Dept-" + random.nextInt(100));
                    }
                    employee.setUpdatedAt(LocalDateTime.now());
                    batch.add(employee);

                    if (batch.size() == 50) {
                        session.updateMultiple(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    session.updateMultiple(batch);
                }
            });
        });
        return updatedCount.get();
    }

    /** Reads data into a DTO projection */
    public List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch) {
        var result = new AtomicReference<List<EmployeeRelationView>>(List.of());
        service.executeReadOnly(session -> {
            var query = session.createQuery("""
                    select new org.benchmark.common.EmployeeRelationView(
                        e.id,
                        e.name,
                        c.name,
                        s.name
                    )
                    from HibEmployee e
                    join e.city c
                    left join e.superior s
                    """, EmployeeRelationView.class);

            stopwatch.benchmark(() -> result.set(query.list()));
        });
        return result.get();
    }

    /** Reads full entities including mapped relations using fetch joins */
    @Override
    public List<Employee> testReadRelatedEntities(Stopwatch stopwatch) {
        var result = new AtomicReference<List<Employee>>(List.of());
        service.executeReadOnly(session -> {
            var query = session.createQuery("""
                    select e
                    from HibEmployee e
                    join fetch e.city c
                    left join fetch e.superior s
                    """, Employee.class);

            stopwatch.benchmark(() -> result.set(query.list()));
        });
        return result.get();
    }

    /** Creates a random employee instance */
    public static Employee createRandomEmployee(City city) {
        var result = new Employee();
        result.setName("Name");
        result.setCity(city);
        result.setContractDay(LocalDate.now());
        result.setIsActive(true);
        result.setEmail("test@example.com");
        result.setPhone("123456789");
        result.setSalary(BigDecimal.valueOf(50000));
        result.setDepartment("IT");
        result.setCreatedAt(LocalDateTime.now());
        result.setUpdatedAt(LocalDateTime.now());
        result.setCreatedBy("System");
        result.setUpdatedBy("System");
        return result;
    }
}