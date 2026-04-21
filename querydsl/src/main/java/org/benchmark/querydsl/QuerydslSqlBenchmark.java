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
            var templates = new H2Templates();
            var configuration = new Configuration(templates);
            this.queryFactory = new SQLQueryFactory(configuration, () -> connection);
        }

        public void insert(Employee employee) {
            var result = queryFactory.insert(qEmployee)
                    .populate(employee)
                    .executeWithKey(Long.class);
            employee.setId(result);
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
    public void testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    dao.insert(createRandomEmployee(city));
                }
            });
        });
    }

    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var city = dao.getCity(1L);
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;

            stopwatch.benchmark(() -> {
                var insertClause = queryFactory.insert(qEmployee);
                var count = 0;

                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    insertClause.populate(createRandomEmployee(city)).addBatch();
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

    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;
            var employees = dao.streamAllEmployees().toList();

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
    }

    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var qEmployee = QEmployee.employee;
            var employees = dao.streamAllEmployees().toList();

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
    }

    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var e = QEmployee.employee;
            var c = QCity.city;
            var s = new QEmployee("superior");

            stopwatch.benchmark(() -> {
                var result = queryFactory.select(Projections.constructor(EmployeeRelationView.class,
                                e.id, e.name, c.name, s.name))
                        .from(e)
                        .join(c).on(e.cityId.eq(c.id))
                        .leftJoin(s).on(e.superiorId.eq(s.id))
                        .fetch();
            });
        });
    }

    @Override
    public void testReadRelatedEntities(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var queryFactory = dao.getQueryFactory();
            var e = QEmployee.employee;
            var c = QCity.city;
            var s = new QEmployee("superior");

            stopwatch.benchmark(() -> {
                var result = queryFactory.select(
                                e.id, e.name, e.contractDay, e.isActive, e.email, e.phone, e.salary, e.department, e.createdAt, e.updatedAt, e.createdBy, e.updatedBy,
                                c.id, c.name, c.countryCode, c.latitude, c.longitude, c.createdAt, c.updatedAt, c.createdBy, c.updatedBy,
                                s.id, s.name
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
                        .toList();
            });
        });
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