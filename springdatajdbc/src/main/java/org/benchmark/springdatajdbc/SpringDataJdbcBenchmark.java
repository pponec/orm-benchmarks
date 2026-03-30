package org.benchmark.springdatajdbc;

import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.OrmBenchmark;
import org.benchmark.common.Stopwatch;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.AbstractJdbcConfiguration;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.CrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Main benchmark class for Spring Data JDBC */
public class SpringDataJdbcBenchmark implements OrmBenchmark {

    private static final int BATCH_SIZE = 50;

    /** City entity mapping */
    @Table("CITY")
    @Getter
    @Setter
    public static class City {
        @Id private Long id;
        private String name;
        @Column("COUNTRY_CODE") private String countryCode;
        private BigDecimal latitude;
        private BigDecimal longitude;
        @Column("CREATED_AT") private LocalDateTime createdAt;
        @Column("UPDATED_AT") private LocalDateTime updatedAt;
        @Column("CREATED_BY") private String createdBy;
        @Column("UPDATED_BY") private String updatedBy;
    }

    /** Employee entity mapping */
    @Table("EMPLOYEE")
    @Getter
    @Setter
    public static class Employee {
        @Id private Long id;
        private String name;

        @Column("SUPERIOR_ID")
        private Long superiorId;

        @Column("CITY_ID")
        private Long cityId;

        @Column("CONTRACT_DAY") private LocalDate contractDay;
        @Column("IS_ACTIVE") private Boolean isActive;
        private String email;
        private String phone;
        private BigDecimal salary;
        private String department;
        @Column("CREATED_AT") private LocalDateTime createdAt;
        @Column("UPDATED_AT") private LocalDateTime updatedAt;
        @Column("CREATED_BY") private String createdBy;
        @Column("UPDATED_BY") private String updatedBy;
    }

    /** Repository interface for Employee */
    public interface EmployeeRepository extends CrudRepository<Employee, Long> {
        /** Retrieves all employees as a stream */
        @Query("SELECT * FROM EMPLOYEE")
        Stream<Employee> streamAll();
    }

    /** Repository interface for City */
    public interface CityRepository extends CrudRepository<City, Long> {
    }

    /** Data Access Object for entities */
    public record Dao(
            /** Returns the employee repository */
            EmployeeRepository employeeRepository,

            /** Returns the city repository */
            CityRepository cityRepository,

            /** Returns the JDBC template */
            JdbcTemplate jdbcTemplate,

            /** Returns NamedParameterJdbcOperations for explicit batch updates */
            NamedParameterJdbcOperations namedParameterJdbcOperations
    ) {
        /** Persists a new entity to the database */
        public void insert(Employee employee) {
            employeeRepository.save(employee);
        }

        /** Persists multiple entities in a batch */
        public void insertMultiple(Iterable<Employee> employees) {
            employeeRepository.saveAll(employees);
        }

        /**
         * Executes an explicit batch update for specific fields.
         * Bypasses CrudRepository.saveAll() to force JDBC batching and partial updates.
         */
        public void batchUpdateSpecific(List<Employee> batch) {
            var sql = "UPDATE EMPLOYEE SET SALARY = :salary, UPDATED_AT = :updatedAt WHERE ID = :id";
            var params = new SqlParameterSource[batch.size()];
            for (int i = 0; i < batch.size(); i++) {
                var emp = batch.get(i);
                params[i] = new MapSqlParameterSource()
                        .addValue("salary", emp.getSalary())
                        .addValue("updatedAt", emp.getUpdatedAt())
                        .addValue("id", emp.getId());
            }
            namedParameterJdbcOperations.batchUpdate(sql, params);
        }

        /**
         * Executes an explicit batch update for random fields.
         */
        public void batchUpdateRandom(List<Employee> batch) {
            var sql = "UPDATE EMPLOYEE SET IS_ACTIVE = :isActive, DEPARTMENT = :department, UPDATED_AT = :updatedAt WHERE ID = :id";
            var params = new SqlParameterSource[batch.size()];
            for (int i = 0; i < batch.size(); i++) {
                var emp = batch.get(i);
                params[i] = new MapSqlParameterSource()
                        .addValue("isActive", emp.getIsActive())
                        .addValue("department", emp.getDepartment())
                        .addValue("updatedAt", emp.getUpdatedAt())
                        .addValue("id", emp.getId());
            }
            namedParameterJdbcOperations.batchUpdate(sql, params);
        }

        /** Retrieves all employees as a stream for efficient processing */
        public Stream<Employee> streamAllEmployees() {
            return employeeRepository.streamAll();
        }

        /** Retrieves a City from the database */
        public City getCity(Long id) {
            var result = cityRepository.findById(id).orElse(null);
            if (result == null) {
                throw new IllegalStateException("City with ID " + id + " not found!");
            }
            return result;
        }

        /** Retrieves employee relations using JdbcTemplate for optimal DTO projection */
        public List<EmployeeRelationView> getEmployeeRelations() {
            var sql = "SELECT E.ID" +
                    ", E.NAME" +
                    ", C.NAME AS CITY_NAME" +
                    ", S.NAME AS SUPERIOR_NAME " +
                    "FROM EMPLOYEE E " +
                    "JOIN CITY C ON E.CITY_ID = C.ID " +
                    "LEFT JOIN EMPLOYEE S ON E.SUPERIOR_ID = S.ID";
            return jdbcTemplate.query(sql, (rs, rowNum) -> new EmployeeRelationView(
                    rs.getLong("ID"),
                    rs.getString("NAME"),
                    rs.getString("CITY_NAME"),
                    rs.getString("SUPERIOR_NAME")
            ));
        }
    }

    /** Configuration for Spring Data JDBC */
    @Configuration
    @EnableJdbcRepositories(considerNestedRepositories = true)
    public static class JdbcConfig extends AbstractJdbcConfiguration {
        /** Wrapped DataSource using DatabaseUtils.getConnection() */
        @Bean
        public DataSource dataSource() {
            return new AbstractDataSource() {
                @Override
                public Connection getConnection() throws SQLException {
                    return DatabaseUtils.getConnection();
                }

                @Override
                public Connection getConnection(String username, String password) throws SQLException {
                    return getConnection();
                }
            };
        }

        /** Configures JDBC operations */
        @Bean
        public NamedParameterJdbcOperations namedParameterJdbcOperations(DataSource dataSource) {
            return new NamedParameterJdbcTemplate(dataSource);
        }

        /** Configures the JDBC template */
        @Bean
        public JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        /** Configures transaction manager */
        @Bean
        public PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    /** Service layer managing context and transactions */
    public static class Service implements AutoCloseable {
        private final AnnotationConfigApplicationContext context;
        private final PlatformTransactionManager transactionManager;
        private final Dao dao;

        public Service() {
            this.context = new AnnotationConfigApplicationContext(JdbcConfig.class);
            this.transactionManager = context.getBean(PlatformTransactionManager.class);
            this.dao = new Dao(
                    context.getBean(EmployeeRepository.class),
                    context.getBean(CityRepository.class),
                    context.getBean(JdbcTemplate.class),
                    context.getBean(NamedParameterJdbcOperations.class)
            );
        }

        /** Executes the given action inside a managed database transaction */
        public void executeInTransaction(Consumer<Dao> action) {
            var transactionTemplate = new TransactionTemplate(transactionManager);
            transactionTemplate.execute(status -> {
                action.accept(dao);
                return null;
            });
        }

        /** Executes the given action in a read-only transaction */
        public void executeReadOnly(Consumer<Dao> action) {
            var transactionTemplate = new TransactionTemplate(transactionManager);
            transactionTemplate.setReadOnly(true);
            transactionTemplate.execute(status -> {
                action.accept(dao);
                return null;
            });
        }

        @Override
        public void close() {
            context.close();
        }
    }

    private final Service service;

    public SpringDataJdbcBenchmark() {
        this.service = new Service();
    }

    /** Executes a single row insert test */
    public void testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    dao.insert(employee);
                }
            });
        });
    }

    /** Executes a batch insert test */
    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(BATCH_SIZE);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    batch.add(createRandomEmployee(city));
                    if (i % BATCH_SIZE == 0) {
                        dao.insertMultiple(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.insertMultiple(batch);
                }
            });
        });
    }

    /** Executes updates on selected columns */
    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            stopwatch.benchmark(() -> {
                var count = 0;
                var batch = new ArrayList<Employee>(BATCH_SIZE);
                try (var stream = dao.streamAllEmployees()) {
                    for (var employee : (Iterable<Employee>) stream::iterator) {
                        employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                        employee.setUpdatedAt(LocalDateTime.now());
                        batch.add(employee);

                        if (++count % BATCH_SIZE == 0) {
                            dao.batchUpdateSpecific(batch);
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    dao.batchUpdateSpecific(batch);
                }
            });
        });
    }

    /** Executes updates on randomly modified columns */
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction(dao -> {
            stopwatch.benchmark(() -> {
                var count = 0;
                var batch = new ArrayList<Employee>(BATCH_SIZE);
                try (var stream = dao.streamAllEmployees()) {
                    for (var employee : (Iterable<Employee>) stream::iterator) {
                        if (random.nextBoolean()) {
                            employee.setIsActive(!employee.getIsActive());
                        } else {
                            employee.setDepartment("Dept-" + random.nextInt(100));
                        }
                        employee.setUpdatedAt(LocalDateTime.now());
                        batch.add(employee);

                        if (++count % BATCH_SIZE == 0) {
                            dao.batchUpdateRandom(batch);
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    dao.batchUpdateRandom(batch);
                }
            });
        });
    }

    /** Reads data including mapped relations */
    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeReadOnly(dao -> {
            stopwatch.benchmark(dao::getEmployeeRelations);
        });
    }

    /** Creates a random employee instance */
    public static Employee createRandomEmployee(City city) {
        var result = new Employee();
        result.setName("Name");
        result.setCityId(city.getId());
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