package org.benchmark.querydsl;

import com.querydsl.core.types.Projections;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.H2Templates;
import com.querydsl.sql.PostgreSQLTemplates;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.List;
import java.util.stream.Stream;

/**
 * Main benchmark class for QueryDSL SQL.
 * Uses QueryDSL's fluent, type-safe SQL API with generated Q-types.
 */
public class QuerydslSqlBenchmark implements OrmBenchmark {

    @Getter @Setter
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

    @Getter @Setter
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

    @Getter @Setter
    public static class RichEmployee {
        private Long id;
        private String name;
        private City city;
        private RichEmployee superior;
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

    public static class Dao {
        private final SQLQueryFactory queryFactory;
        private final QEmployee qEmployee = QEmployee.employee;
        private final QCity qCity = QCity.city;

        public Dao(Connection connection) {
            var templates = DatabaseUtils.isPostgresProfile() ? new PostgreSQLTemplates() : new H2Templates();
            var configuration = new Configuration(templates);
            this.queryFactory = new SQLQueryFactory(configuration, () -> connection);
        }

        public void insert(Employee employee) {
            queryFactory.insert(qEmployee)
                    .populate(employee)
                    .execute();
        }

        public Stream<Employee> streamAllEmployees() {
            return queryFactory.select(Projections.bean(Employee.class, qEmployee.all()))
                    .from(qEmployee)
                    .stream();
        }

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

        public SQLQueryFactory getQueryFactory() {
            return queryFactory;
        }

        public long countEmployees() {
            return queryFactory.select(qEmployee.id.count())
                    .from(qEmployee)
                    .fetchOne();
        }
    }

    public static class Service {
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

    @Override
    public int testSingleInsert(Stopwatch stopwatch) {
        var rowsBeforeMeasurement = new AtomicReference<Long>(0L);
        service.executeInTransaction(dao -> rowsBeforeMeasurement.set(dao.countEmployees()));

        stopwatch.benchmark(() -> service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    dao.insert(employee);
                }
            })
        );

        var rowsAfterMeasurement = new AtomicReference<Long>(0L);
        service.executeInTransaction(dao -> rowsAfterMeasurement.set(dao.countEmployees()));
        var insertedRows = rowsAfterMeasurement.get() - rowsBeforeMeasurement.get();
        if (insertedRows != stopwatch.getIterations()) {
            throw new IllegalStateException(
                    "Insert validation failed for QueryDSL single insert: expected %s inserted rows, got %s."
                            .formatted(stopwatch.getIterations(), insertedRows)
            );
        }
        return Math.toIntExact(insertedRows);
    }

    public int testBatchInsert(Stopwatch stopwatch) {
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
        return stopwatch.getIterations();
    }

    public int testSpecificUpdate(Stopwatch stopwatch) {
        var updatedCount = new AtomicReference<>(0);
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;
            var employees = dao.streamAllEmployees().toList();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var updateClause = queryFactory.update(qEmployee);
                var count = 0;

                for (var employee : employees) {
                    var newSalary = employee.getSalary().add(BigDecimal.valueOf(1000));
                    updateClause.set(qEmployee.salary, newSalary)
                            .set(qEmployee.updatedAt, LocalDateTime.now())
                            .where(qEmployee.id.eq(employee.getId()))
                            .addBatch();

                    if (++count % 50 == 0) {
                        updateClause.execute();
                        updateClause = queryFactory.update(qEmployee);
                    }
                }
                if (count % 50 != 0) {
                    updateClause.execute();
                }
            });
        });
        return updatedCount.get();
    }

    public int testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        var updatedCount = new AtomicReference<>(0);
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;
            var employees = dao.streamAllEmployees().toList();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var updateClause = queryFactory.update(qEmployee);
                var count = 0;

                for (var employee : employees) {
                    if (random.nextBoolean()) {
                        updateClause.set(qEmployee.isActive, !employee.getIsActive());
                    } else {
                        updateClause.set(qEmployee.department, "Dept-" + random.nextInt(100));
                    }
                    updateClause.set(qEmployee.updatedAt, LocalDateTime.now())
                            .where(qEmployee.id.eq(employee.getId()))
                            .addBatch();

                    if (++count % 50 == 0) {
                        updateClause.execute();
                        updateClause = queryFactory.update(qEmployee);
                    }
                }
                if (count % 50 != 0) {
                    updateClause.execute();
                }
            });
        });
        return updatedCount.get();
    }

    public List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch) {
        var result = new AtomicReference<List<EmployeeRelationView>>(List.of());
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var e = QEmployee.employee;
            var c = QCity.city;
            var s = new QEmployee("superior");

            stopwatch.benchmark(() -> {
                result.set(queryFactory.select(Projections.constructor(EmployeeRelationView.class,
                                e.id, e.name, c.name, s.name))
                        .from(e)
                        .join(c).on(e.cityId.eq(c.id))
                        .leftJoin(s).on(e.superiorId.eq(s.id))
                        .fetch());
            });
        });
        return result.get();
    }

    @Override
    public List<RichEmployee> testReadRelatedEntities(Stopwatch stopwatch) {
        var result = new AtomicReference<List<RichEmployee>>(List.of());
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var e = QEmployee.employee;
            var c = QCity.city;
            var s = new QEmployee("superior");

            stopwatch.benchmark(() -> {
                result.set(queryFactory.select(
                                e.id, e.name, e.contractDay, e.isActive, e.email, e.phone, e.salary, e.department, e.createdAt, e.updatedAt, e.createdBy, e.updatedBy,
                                c.id, c.name, c.countryCode, c.latitude, c.longitude, c.createdAt, c.updatedAt, c.createdBy, c.updatedBy,
                                s.id, s.name, s.contractDay, s.isActive, s.email, s.phone, s.salary, s.department, s.createdAt, s.updatedAt, s.createdBy, s.updatedBy
                        )
                        .from(e)
                        .join(c).on(e.cityId.eq(c.id))
                        .leftJoin(s).on(e.superiorId.eq(s.id))
                        .fetch()
                        .stream()
                        .map(t -> {
                            var city = new City();
                            city.setId(t.get(c.id));
                            city.setName(t.get(c.name));
                            city.setCountryCode(t.get(c.countryCode));
                            city.setLatitude(t.get(c.latitude));
                            city.setLongitude(t.get(c.longitude));
                            city.setCreatedAt(t.get(c.createdAt));
                            city.setUpdatedAt(t.get(c.updatedAt));
                            city.setCreatedBy(t.get(c.createdBy));
                            city.setUpdatedBy(t.get(c.updatedBy));

                            RichEmployee sup = null;
                            if (t.get(s.id) != null) {
                                sup = new RichEmployee();
                                sup.setId(t.get(s.id));
                                sup.setName(t.get(s.name));
                                sup.setContractDay(t.get(s.contractDay));
                                sup.setIsActive(t.get(s.isActive));
                                sup.setEmail(t.get(s.email));
                                sup.setPhone(t.get(s.phone));
                                sup.setSalary(t.get(s.salary));
                                sup.setDepartment(t.get(s.department));
                                sup.setCreatedAt(t.get(s.createdAt));
                                sup.setUpdatedAt(t.get(s.updatedAt));
                                sup.setCreatedBy(t.get(s.createdBy));
                                sup.setUpdatedBy(t.get(s.updatedBy));
                            }

                            var emp = new RichEmployee();
                            emp.setId(t.get(e.id));
                            emp.setName(t.get(e.name));
                            emp.setCity(city);
                            emp.setSuperior(sup);
                            emp.setContractDay(t.get(e.contractDay));
                            emp.setIsActive(t.get(e.isActive));
                            emp.setEmail(t.get(e.email));
                            emp.setPhone(t.get(e.phone));
                            emp.setSalary(t.get(e.salary));
                            emp.setDepartment(t.get(e.department));
                            emp.setCreatedAt(t.get(e.createdAt));
                            emp.setUpdatedAt(t.get(e.updatedAt));
                            emp.setCreatedBy(t.get(e.createdBy));
                            emp.setUpdatedBy(t.get(e.updatedBy));
                            return emp;
                        })
                        .toList());
            });
        });
        return result.get();
    }

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