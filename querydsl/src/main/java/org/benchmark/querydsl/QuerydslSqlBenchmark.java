package org.benchmark.querydsl;

import com.querydsl.core.types.Projections;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.H2Templates;
import com.querydsl.sql.SQLQueryFactory;
import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.OrmBenchmark;
import org.benchmark.common.Stopwatch;
import org.benchmark.querydsl.sql.model.QCity;
import org.benchmark.querydsl.sql.model.QEmployee;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Stream;

/** Main benchmark class for QueryDSL SQL */
public class QuerydslSqlBenchmark implements OrmBenchmark {

    /** City entity as a plain POJO */
    @Getter
    @Setter
    public static class City {
        private Long id;
        private String name;
        private String countryCode;
        private BigDecimal latitude;
        private BigDecimal longitude;
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;
        private String createdBy;
        private String updatedBy;
    }

    /** Employee entity as a plain POJO */
    @Getter
    @Setter
    public static class Employee {
        private Long id;
        private String name;
        private Long superiorId;
        private Long cityId;
        private LocalDate contractDay;
        private Boolean isActive;
        private String email;
        private String phone;
        private BigDecimal salary;
        private String department;
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;
        private String createdBy;
        private String updatedBy;
    }

    /** Data Access Object for direct SQL queries */
    public static class Dao {
        private final SQLQueryFactory queryFactory;
        private final QEmployee qEmployee = QEmployee.employee;
        private final QCity qCity = QCity.city;

        public Dao(Connection connection) {
            var templates = new H2Templates();
            var configuration = new Configuration(templates);
            this.queryFactory = new SQLQueryFactory(configuration, () -> connection);
        }

        /** Inserts a new entity using bean population */
        public void insert(Employee employee) {
            var result = queryFactory.insert(qEmployee)
                    .populate(employee)
                    .executeWithKey(Long.class);
            employee.setId(result);
        }

        /** Retrieves all employees as a stream */
        public Stream<Employee> streamAllEmployees() {
            return queryFactory.select(Projections.bean(Employee.class, qEmployee.all()))
                    .from(qEmployee)
                    .stream();
        }

        /** Retrieves a City directly via SQL */
        public City getCity(Long id) {
            var result = queryFactory.select(Projections.bean(City.class, qCity.all()))
                    .from(qCity)
                    .where(qCity.id.eq(id))
                    .fetchOne();

            if (result == null) {
                throw new IllegalStateException("City with ID " + id + " not found.");
            }
            return result;
        }

        /** Provides access to the query factory for custom batching */
        public SQLQueryFactory getQueryFactory() {
            return queryFactory;
        }
    }

    /** Service layer managing explicit JDBC transactions */
    public static class Service {

        /** Executes the given action inside a managed JDBC transaction */
        public void executeInTransaction(Consumer<Dao> action) {
            try (var connection = DatabaseUtils.getConnection()) {
                connection.setAutoCommit(false);
                try {
                    action.accept(new Dao(connection));
                    connection.commit();
                } catch (Exception e) {
                    connection.rollback();
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Service service;

    public QuerydslSqlBenchmark() {
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

    /** Executes a batch insert test using JDBC batching */
    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;

            stopwatch.benchmark(() -> {
                var insertClause = queryFactory.insert(qEmployee);
                var count = 0;

                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    insertClause.populate(employee).addBatch();

                    if (++count % 50 == 0) {
                        insertClause.execute();
                        insertClause = queryFactory.insert(qEmployee);
                    }
                }
                if (count % 50 != 0) {
                    insertClause.execute();
                }
            });
        });
    }

    /** Executes updates by firing specific UPDATE statements */
    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;

            stopwatch.benchmark(() -> {
                var updateClause = queryFactory.update(qEmployee);
                var count = 0;

                try (var stream = dao.streamAllEmployees()) {
                    for (var employee : (Iterable<Employee>) stream::iterator) {
                        var newSalary = employee.getSalary().add(BigDecimal.valueOf(1000));
                        var newUpdatedAt = LocalDateTime.now();

                        updateClause.set(qEmployee.salary, newSalary)
                                .set(qEmployee.updatedAt, newUpdatedAt)
                                .where(qEmployee.id.eq(employee.getId()))
                                .addBatch();

                        if (++count % 50 == 0) {
                            updateClause.execute();
                            updateClause = queryFactory.update(qEmployee);
                        }
                    }
                }
                if (count % 50 != 0) {
                    updateClause.execute();
                }
            });
        });
    }

    /** Executes random column updates via SQL batches */
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;

            stopwatch.benchmark(() -> {
                var count = 0;
                try (var stream = dao.streamAllEmployees()) {
                    for (var employee : (Iterable<Employee>) stream::iterator) {
                        var updateClause = queryFactory.update(qEmployee);

                        if (random.nextBoolean()) {
                            updateClause.set(qEmployee.isActive, !employee.getIsActive());
                        } else {
                            updateClause.set(qEmployee.department, "Dept-" + random.nextInt(100));
                        }

                        updateClause.set(qEmployee.updatedAt, LocalDateTime.now())
                                .where(qEmployee.id.eq(employee.getId()))
                                .execute();

                        count++;
                    }
                }
            });
        });
    }

    /** Reads data using explicit explicit SQL JOINs */
    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var e = QEmployee.employee;
            var c = QCity.city;
            var s = new QEmployee("superior");

            stopwatch.benchmark(() -> {
                queryFactory.select(Projections.constructor(EmployeeRelationView.class,
                                e.id, e.name, c.name, s.name))
                        .from(e)
                        .join(c).on(e.cityId.eq(c.id))
                        .leftJoin(s).on(e.superiorId.eq(s.id))
                        .fetch();
            });
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