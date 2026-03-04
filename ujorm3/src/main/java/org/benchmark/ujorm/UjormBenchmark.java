package org.benchmark.ujorm;

import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.Stopwatch;
import org.ujorm.core.AbstractSnapshotable;
import org.ujorm.mapper.core.EntityManager;
import org.ujorm.mapper.jdbc.ResultSetMapper;
import org.ujorm.tools.jdbc.SqlParamBuilder;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Main benchmark class for Ujorm3 */
public class UjormBenchmark {

    /** City entity mapping */
    @Table(name = "city")
    @Getter
    @Setter
    public static class City extends AbstractSnapshotable<City> {
        @Id private Long id;
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
    @Table(name = "employee")
    @Getter
    @Setter
    public static class Employee extends AbstractSnapshotable<Employee> {
        @Id private Long id;
        private String name;

        @JoinColumn(name = "superior_id")
        private Employee superior;

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
        private final EntityManager<Employee, Long> empEm = EntityManager.of(Employee.class, Long.class);
        private final EntityManager<City, Long> cityEm = EntityManager.of(City.class, Long.class);
        private final ResultSetMapper<EmployeeRelationView> empView = ResultSetMapper.of(EmployeeRelationView.class);

        /** Persists a single entity to the database */
        public void insert(Employee entity, Connection conn) {
            empEm.crud(conn).insert(entity);
        }

        /** Persists a batch of entities */
        public void insertBatch(List<Employee> entities, Connection conn) {
            empEm.crud(conn).insertBatch(entities.toArray(Employee[]::new));
        }

        /** Retrieves all employees */
        public List<Employee> findAllEmployees(Connection conn) {
            return empEm.crud(conn).read("").streamMap(empEm::map).toList();
        }

        /** Updates specific columns using batch */
        public void updateSalaryBatch(List<Employee> entities, Connection conn) {
            empEm.crud(conn).updateBatch(entities, "salary", "updatedAt");
        }

        /** Updates entities with changed snapshot detection */
        public void updateChangedBatch(List<Employee> entities, Connection conn) {
            var array = entities.toArray(new Employee[0]);
            empEm.crud(conn).updateChanged(array);
        }

        /** Retrieves a City from the database */
        public City getCity(Long id, Connection conn) {
            return cityEm.crud(conn).read(id).orElse(null);
        }

        /** Retrieves employees joined with city and superior */
        public List<EmployeeRelationView> findWithRelations(Connection conn) {
            var sql = """
                SELECT e.id
                , e.name
                , c.name AS "cityName"
                , s.name AS "superiorName" 
                FROM employee e 
                JOIN city c ON e.city_id = c.id 
                LEFT JOIN employee s ON e.superior_id = s.id
            """;
            try (var builder = new SqlParamBuilder(conn)) {
                return builder.sql(sql).streamMap(empView::map).toList();
            }
        }
    }

    /** Service layer managing transactions */
    public static class Service {

        /** Executes the given action inside a managed database transaction */
        public void executeInTransaction(java.util.function.BiConsumer<Dao, Connection> action) {
            try (var conn = DatabaseUtils.getConnection()) {
                conn.setAutoCommit(false);
                var dao = new Dao();
                try {
                    action.accept(dao, conn);
                    conn.commit();
                } catch (Exception e) {
                    conn.rollback();
                    throw new RuntimeException("Transaction failed", e);
                }
            } catch (java.sql.SQLException e) {
                throw new RuntimeException("Failed to obtain connection", e);
            }
        }

        /** Executes the given action in a read-only transaction */
        public void executeReadOnly(java.util.function.BiConsumer<Dao, Connection> action) {
            try (var conn = DatabaseUtils.getConnection()) {
                conn.setReadOnly(true);
                var dao = new Dao();
                action.accept(dao, conn);
            } catch (java.sql.SQLException e) {
                throw new RuntimeException("Failed to obtain connection", e);
            }
        }
    }

    private final Service service;

    public UjormBenchmark() {
        this.service = new Service();
    }

    /** Executes a single row insert test */
    public void testSingleInsert(Stopwatch stopwatch) {
        stopwatch.start();
        service.executeInTransaction((dao, conn) -> {
            var city = dao.getCity(1L, conn);
            for (var i = 1; i <= 100_000; i++) {
                var employee = createRandomEmployee(city);
                dao.insert(employee, conn);
            }
        });
        stopwatch.stop();
    }

    /** Executes a batch insert test */
    public void testBatchInsert(Stopwatch stopwatch) {
        stopwatch.start();
        service.executeInTransaction((dao, conn) -> {
            var city = dao.getCity(1L, conn);
            var batch = new ArrayList<Employee>(50);
            for (var i = 1; i <= 100_000; i++) {
                batch.add(createRandomEmployee(city));
                if (i % 50 == 0) {
                    dao.insertBatch(batch, conn);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                dao.insertBatch(batch, conn);
            }
        });
        stopwatch.stop();
    }

    /** Executes updates on selected columns */
    public void testSpecificUpdate(Stopwatch stopwatch) {
        stopwatch.start();
        service.executeInTransaction((dao, conn) -> {
            var employees = dao.findAllEmployees(conn);
            var batch = new ArrayList<Employee>(50);
            for (var employee : employees) {
                employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                employee.setUpdatedAt(LocalDateTime.now());
                batch.add(employee);
                if (batch.size() == 50) {
                    dao.updateSalaryBatch(batch, conn);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                dao.updateSalaryBatch(batch, conn);
            }
        });
        stopwatch.stop();
    }

    /** Executes updates on randomly modified columns */
    public void testRandomUpdate(Stopwatch stopwatch) {
        stopwatch.start();
        var random = new Random();
        service.executeInTransaction((dao, conn) -> {
            var employees = dao.findAllEmployees(conn);
            var batch = new ArrayList<Employee>(50);
            for (var employee : employees) {
                employee.saveSnapshot();
                if (random.nextBoolean()) {
                    employee.setIsActive(!employee.getIsActive());
                } else {
                    employee.setDepartment("Dept-" + random.nextInt(100));
                }
                employee.setUpdatedAt(LocalDateTime.now());
                batch.add(employee);

                if (batch.size() == 50) {
                    dao.updateChangedBatch(batch, conn);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                dao.updateChangedBatch(batch, conn);
            }
        });
        stopwatch.stop();
    }

    /** Reads data including mapped relations */
    public void testReadWithRelations(Stopwatch stopwatch) {
        stopwatch.start();
        service.executeReadOnly((dao, conn) -> {
            var result = dao.findWithRelations(conn);
        });
        stopwatch.stop();
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