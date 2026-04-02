package org.benchmark.jooq;

import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.OrmBenchmark;
import org.benchmark.common.Stopwatch;
import org.benchmark.jooq.generated.tables.records.EmployeeRecord;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.benchmark.jooq.generated.Tables.CITY;
import static org.benchmark.jooq.generated.Tables.EMPLOYEE;

/** Main benchmark class for jOOQ */
public class JooqBenchmark implements OrmBenchmark {

    private static final int BATCH_SIZE = 50;

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

    public static class Service implements AutoCloseable {
        public void execute(Consumer<DSLContext> action) {
            try (var connection = DatabaseUtils.getConnection()) {
                var dsl = DSL.using(connection, SQLDialect.H2);
                action.accept(dsl);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /** Executes the given action inside a transaction */
        public void executeInTransaction(Consumer<DSLContext> action) {
            try (var connection = DatabaseUtils.getConnection()) {
                var dsl = DSL.using(connection, SQLDialect.H2);
                dsl.transaction(configuration -> {
                    var transactionalDsl = DSL.using(configuration);
                    action.accept(transactionalDsl);
                });
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
        }
    }

    private final Service service;

    public JooqBenchmark() {
        this.service = new Service();
    }

    public void testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dsl -> {
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    dsl.insertInto(EMPLOYEE)
                            .set(createRandomEmployeeRecord(1L))
                            .execute();
                }
            });
        });
    }

    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dsl -> {
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<EmployeeRecord>(BATCH_SIZE);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    batch.add(createRandomEmployeeRecord(1L));
                    if (i % BATCH_SIZE == 0) {
                        dsl.batchInsert(batch).execute();
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dsl.batchInsert(batch).execute();
                }
            });
        });
    }

    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction(dsl -> {
            stopwatch.benchmark(() -> {
                var records = dsl.selectFrom(EMPLOYEE).fetch();
                for (var record : records) {
                    record.setSalary(record.getSalary().add(BigDecimal.valueOf(1000)));
                    record.setUpdatedAt(LocalDateTime.now());
                }
                dsl.batchUpdate(records).execute();
            });
        });
    }

    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction(dsl -> {
            stopwatch.benchmark(() -> {
                var records = dsl.selectFrom(EMPLOYEE).fetch();
                for (var record : records) {
                    if (random.nextBoolean()) {
                        record.setIsActive(!record.getIsActive());
                    } else {
                        record.setDepartment("Dept-" + random.nextInt(100));
                    }
                    record.setUpdatedAt(LocalDateTime.now());
                }
                dsl.batchUpdate(records).execute();
            });
        });
    }

    public void testReadWithRelations(Stopwatch stopwatch) {
        service.execute(dsl -> {
            var superior = EMPLOYEE.as("superior");
            stopwatch.benchmark(() -> {
                var result = dsl.select(EMPLOYEE.ID, EMPLOYEE.NAME, CITY.NAME, superior.NAME)
                        .from(EMPLOYEE)
                        .join(CITY).on(EMPLOYEE.CITY_ID.eq(CITY.ID))
                        .leftJoin(superior).on(EMPLOYEE.SUPERIOR_ID.eq(superior.ID))
                        .fetchInto(EmployeeRelationView.class);
            });
        });
    }

    @Override
    public void testReadRelatedEntities(Stopwatch stopwatch) {
        service.execute(dsl -> {
            var superior = EMPLOYEE.as("superior");
            stopwatch.benchmark(() -> {
                List<RichEmployee> result = dsl.select(EMPLOYEE.fields())
                        .select(CITY.fields())
                        .select(superior.ID, superior.NAME)
                        .from(EMPLOYEE)
                        .join(CITY).on(EMPLOYEE.CITY_ID.eq(CITY.ID))
                        .leftJoin(superior).on(EMPLOYEE.SUPERIOR_ID.eq(superior.ID))
                        .fetch(r -> {
                            var city = new City();
                            city.setId(r.get(CITY.ID));
                            city.setName(r.get(CITY.NAME));
                            city.setCountryCode(r.get(CITY.COUNTRY_CODE));
                            city.setLatitude(r.get(CITY.LATITUDE));
                            city.setLongitude(r.get(CITY.LONGITUDE));
                            city.setCreatedAt(r.get(CITY.CREATED_AT));
                            city.setUpdatedAt(r.get(CITY.UPDATED_AT));
                            city.setCreatedBy(r.get(CITY.CREATED_BY));
                            city.setUpdatedBy(r.get(CITY.UPDATED_BY));

                            RichEmployee sup = null;
                            if (r.get(superior.ID) != null) {
                                sup = new RichEmployee();
                                sup.setId(r.get(superior.ID));
                                sup.setName(r.get(superior.NAME));
                            }

                            var emp = new RichEmployee();
                            emp.setId(r.get(EMPLOYEE.ID));
                            emp.setName(r.get(EMPLOYEE.NAME));
                            emp.setCity(city);
                            emp.setSuperior(sup);
                            emp.setContractDay(r.get(EMPLOYEE.CONTRACT_DAY));
                            emp.setIsActive(r.get(EMPLOYEE.IS_ACTIVE));
                            emp.setEmail(r.get(EMPLOYEE.EMAIL));
                            emp.setPhone(r.get(EMPLOYEE.PHONE));
                            emp.setSalary(r.get(EMPLOYEE.SALARY));
                            emp.setDepartment(r.get(EMPLOYEE.DEPARTMENT));
                            emp.setCreatedAt(r.get(EMPLOYEE.CREATED_AT));
                            emp.setUpdatedAt(r.get(EMPLOYEE.UPDATED_AT));
                            emp.setCreatedBy(r.get(EMPLOYEE.CREATED_BY));
                            emp.setUpdatedBy(r.get(EMPLOYEE.UPDATED_BY));
                            return emp;
                        });
            });
        });
    }

    public static EmployeeRecord createRandomEmployeeRecord(Long cityId) {
        var result = new EmployeeRecord();
        result.setName("Name");
        result.setCityId(cityId);
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