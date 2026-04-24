package org.benchmark.jdbi;

import lombok.Getter;
import lombok.Setter;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.Stopwatch;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.Nested;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Main benchmark class for JDBI.
 * Uses SQL Object API and explicit SQL statements, which is the idiomatic Jdbi style.
 */
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

    /** Flat Employee entity mapping */
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

    /** Rich Domain Object representing the full object graph (for reads) */
    @Getter
    @Setter
    public static class RichEmployee {
        private Long id;
        private String name;
        @Nested("c_")
        private City city;
        @Nested("s_")
        private Employee superior;
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

    /** Data Access Object for entities */
    @RegisterBeanMapper(Employee.class)
    public interface Dao {

        /** Inserts a new employee and returns the generated ID */
        @SqlUpdate("""
            INSERT INTO employee (
                name, superior_id, city_id, contract_day, is_active, email, phone, salary, department, created_at, updated_at, created_by, updated_by
            ) VALUES (
                :name, :superiorId, :cityId, :contractDay, :isActive, :email, :phone, :salary, :department, :createdAt, :updatedAt, :createdBy, :updatedBy
            )
            """)
        @GetGeneratedKeys("id")
        Long insert(@BindBean Employee employee);

        /** Inserts a batch of employees */
        @SqlBatch("""
            INSERT INTO employee (
                name, superior_id, city_id, contract_day, is_active, email, phone, salary, department, created_at, updated_at, created_by, updated_by
            ) VALUES (
                :name, :superiorId, :cityId, :contractDay, :isActive, :email, :phone, :salary, :department, :createdAt, :updatedAt, :createdBy, :updatedBy
            )
            """)
        void insertBatch(@BindBean List<Employee> employees);

        /** Retrieves all employees */
        @SqlQuery("SELECT * FROM employee")
        List<Employee> findAllEmployees();

        /** Updates salary for a specific employee */
        @SqlUpdate("""
            UPDATE employee
            SET salary = :salary, updated_at = :updatedAt
            WHERE id = :id
            """)
        void updateSalary(@BindBean Employee employee);

        /** Updates salary in batch */
        @SqlBatch("""
            UPDATE employee
            SET salary = :salary, updated_at = :updatedAt
            WHERE id = :id
            """)
        void updateSalaryBatch(@BindBean List<Employee> employees);

        /** Updates active status and department in batch */
        @SqlBatch("""
            UPDATE employee
            SET is_active = :isActive, department = :department, updated_at = :updatedAt
            WHERE id = :id
            """)
        void updateRandomly(@BindBean List<Employee> employees);

        /** Retrieves employees joined with city and superior */
        @SqlQuery("""
            SELECT e.id, e.name, c.name AS city_name, s.name AS superior_name
            FROM employee e
            JOIN city c ON e.city_id = c.id
            LEFT JOIN employee s ON e.superior_id = s.id
            """)
        @RegisterConstructorMapper(EmployeeRelationView.class)
        List<EmployeeRelationView> findWithRelations();

        /** Retrieves full entities mapped into an object graph from a single query */
        @SqlQuery("""
            SELECT e.id
                 , e.name
                 , e.contract_day
                 , e.is_active
                 , e.email
                 , e.phone
                 , e.salary
                 , e.department
                 , e.created_at
                 , e.updated_at
                 , e.created_by
                 , e.updated_by
                 , c.id AS c_id
                 , c.name AS c_name
                 , c.country_code AS c_country_code
                 , c.latitude AS c_latitude
                 , c.longitude AS c_longitude
                 , c.created_at AS c_created_at
                 , c.updated_at AS c_updated_at
                 , c.created_by AS c_created_by
                 , c.updated_by AS c_updated_by
                 , s.id AS s_id
                 , s.name AS s_name
                 , s.superior_id AS s_superior_id
                 , s.city_id AS s_city_id
                 , s.contract_day AS s_contract_day
                 , s.is_active AS s_is_active
                 , s.email AS s_email
                 , s.phone AS s_phone
                 , s.salary AS s_salary
                 , s.department AS s_department
                 , s.created_at AS s_created_at
                 , s.updated_at AS s_updated_at
                 , s.created_by AS s_created_by
                 , s.updated_by AS s_updated_by
            FROM employee e
            JOIN city c ON e.city_id = c.id
            LEFT JOIN employee s ON e.superior_id = s.id
            """)
        @RegisterBeanMapper(RichEmployee.class)
        List<RichEmployee> findEntitiesWithRelations();
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
    @Override
    public int testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(dao -> {
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee();
                    dao.insert(employee);
                }
            });
        });
        return stopwatch.getIterations();
    }

    /** Executes a batch insert test */
    public int testBatchInsert(Stopwatch stopwatch) {
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
        return stopwatch.getIterations();
    }

    /** Executes updates on selected columns */
    public int testSpecificUpdate(Stopwatch stopwatch) {
        var updatedCount = new AtomicReference<>(0);
        service.executeInTransaction(dao -> {
            var employees = dao.findAllEmployees();
            updatedCount.set(employees.size());
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
        return updatedCount.get();
    }

    /** Executes updates on randomly modified columns */
    public int testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        var updatedCount = new AtomicReference<>(0);
        service.executeInTransaction(dao -> {
            var employees = dao.findAllEmployees();
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
                        dao.updateRandomly(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.updateRandomly(batch);
                }
            });
        });
        return updatedCount.get();
    }

    /** Reads data including mapped relations */
    public List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch) {
        var result = new AtomicReference<List<EmployeeRelationView>>(List.of());
        service.executeReadOnly(dao -> {
            stopwatch.benchmark(() -> {
                result.set(dao.findWithRelations());
            });
        });
        return result.get();
    }

    /** Reads full entities including mapped relations */
    @Override
    public List<RichEmployee> testReadRelatedEntities(Stopwatch stopwatch) {
        var result = new AtomicReference<List<RichEmployee>>(List.of());
        service.executeReadOnly(dao -> {
            stopwatch.benchmark(() -> {
                result.set(dao.findEntitiesWithRelations());
            });
        });
        return result.get();
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
        result.setSalary(BigDecimal.valueOf(50_000));
        result.setDepartment("IT");
        result.setCreatedAt(LocalDateTime.now());
        result.setUpdatedAt(LocalDateTime.now());
        result.setCreatedBy("System");
        result.setUpdatedBy("System");
        return result;
    }
}