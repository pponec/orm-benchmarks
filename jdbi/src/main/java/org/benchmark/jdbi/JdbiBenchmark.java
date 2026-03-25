package org.benchmark.jdbi;

import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.Stopwatch;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.benchmark.common.OrmBenchmark;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

/** Main benchmark class for JDBI */
public class JdbiBenchmark implements OrmBenchmark {

    /** City entity mapping */
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

    /** Employee entity mapping */
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

    /** Data transfer object for employee relations */
    public record EmployeeRelationView(
            /** The ID of the employee */
            Long id,
            /** The name of the employee */
            String name,
            /** The name of the city */
            String cityName,
            /** The name of the superior */
            String superiorName
    ) {
    }

    /** Data Access Object for entities */
    @RegisterBeanMapper(Employee.class)
    public interface Dao {

        /** Inserts a new employee and returns the generated ID */
        @SqlUpdate("INSERT INTO employee (name, superior_id, city_id, contract_day, is_active, email, phone, salary, department, created_at, updated_at, created_by, updated_by) VALUES (:name, :superiorId, :cityId, :contractDay, :isActive, :email, :phone, :salary, :department, :createdAt, :updatedAt, :createdBy, :updatedBy)")
        @GetGeneratedKeys("id")
        Long insert(@BindBean Employee employee);

        /** Inserts a batch of employees */
        @SqlBatch("INSERT INTO employee (name, superior_id, city_id, contract_day, is_active, email, phone, salary, department, created_at, updated_at, created_by, updated_by) VALUES (:name, :superiorId, :cityId, :contractDay, :isActive, :email, :phone, :salary, :department, :createdAt, :updatedAt, :createdBy, :updatedBy)")
        void insertBatch(@BindBean List<Employee> employees);

        /** Retrieves all employees */
        @SqlQuery("SELECT * FROM employee")
        List<Employee> findAllEmployees();

        /** Updates salary for a specific employee */
        @SqlUpdate("UPDATE employee SET salary = :salary, updated_at = :updatedAt WHERE id = :id")
        void updateSalary(@BindBean Employee employee);

        /** Updates salary in batch */
        @SqlBatch("UPDATE employee SET salary = :salary, updated_at = :updatedAt WHERE id = :id")
        void updateSalaryBatch(@BindBean List<Employee> employees);

        /** Updates active status and department in batch */
        @SqlBatch("UPDATE employee SET is_active = :isActive, department = :department, updated_at = :updatedAt WHERE id = :id")
        void updateRandomly(@BindBean List<Employee> employees);

        /** Retrieves employees joined with city and superior */
        @SqlQuery("SELECT e.id, e.name, c.name AS city_name, s.name AS superior_name FROM employee e JOIN city c ON e.city_id = c.id LEFT JOIN employee s ON e.superior_id = s.id")
        @RegisterConstructorMapper(EmployeeRelationView.class)
        List<EmployeeRelationView> findWithRelations();
    }

    /** Service layer managing transactions */
    public static class Service {
        private final Jdbi jdbi;

        public Service() {
            this.jdbi = Jdbi.create(DatabaseUtils::getConnection);
            this.jdbi.installPlugin(new SqlObjectPlugin());
        }

        /** Executes the given action inside a managed database transaction */
        public void executeInTransaction(Consumer<Dao> action) {
            this.jdbi.useHandle(handle -> {
                try {
                    var dao = handle.attach(Dao.class);
                    action.accept(dao);
                    handle.getConnection().commit();
                } catch (Exception e) {
                    try {
                        handle.getConnection().rollback();
                    } catch (SQLException ex) {
                        e.addSuppressed(ex);
                    }
                    throw new RuntimeException("Transaction failed", e);
                }
            });
        }

        /** Executes the given action in a read-only transaction */
        public void executeReadOnly(Consumer<Dao> action) {
            this.jdbi.useHandle(handle -> {
                try {
                    var connection = handle.getConnection();
                    connection.setReadOnly(true);
                    var dao = handle.attach(Dao.class);
                    action.accept(dao);
                    connection.commit();
                } catch (SQLException e) {
                    throw new RuntimeException("Read-only transaction failed", e);
                }
            });
        }
    }

    private final Service service;

    public JdbiBenchmark() {
        this.service = new Service();
    }

    /** Executes a single row insert test */
    public void testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee();
                    dao.insert(employee);
                }
            });
        });
    }

    /** Executes a batch insert test */
    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    batch.add(createRandomEmployee());
                    if (i % 50 == 0) {
                        dao.insertBatch(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.insertBatch(batch);
                }
            });
        });
    }

    /** Executes updates on selected columns */
    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            var employees = dao.findAllEmployees();
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var employee : employees) {
                    employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                    employee.setUpdatedAt(LocalDateTime.now());
                    batch.add(employee);

                    if (batch.size() == 50) {
                        dao.updateSalaryBatch(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.updateSalaryBatch(batch);
                }
            });
        });
    }

    /** Executes updates on randomly modified columns */
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction(dao -> {
            var employees = dao.findAllEmployees();
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
                        dao.updateRandomly(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.updateRandomly(batch);
                }
            });
        });
    }

    /** Reads data including mapped relations */
    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeReadOnly(dao -> {
            stopwatch.benchmark(() -> {
                var result = dao.findWithRelations();
            });
        });
    }

    /** Creates a random employee instance */
    public static Employee createRandomEmployee() {
        var result = new Employee();
        result.setName("Name");
        result.setCityId(1L);
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