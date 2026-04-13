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
import org.benchmark.ujorm.meta.*;
import org.ujorm.core.AbstractSnapshotable;
import org.ujorm.orm.core.EntityManager;
import org.ujorm.orm.dsl.SelectQuery;
import org.ujorm.orm.jdbc.ResultSetMapper;
import org.ujorm.orm.SqlQuery;
import org.benchmark.common.OrmBenchmark;
import org.ujorm.orm.utils.EntityContext;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/** Main benchmark class for Ujorm3 */
public class UjormBenchmark implements OrmBenchmark {

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

    /** City entity mapping */
    @Table(name = "city")
    @Getter
    @Setter
    public static class Test extends AbstractSnapshotable<City> {
        @Id private long id;
        private boolean enabled;
        private Boolean sex;
        transient private String name;
    }

    /** Data Access Object for entities */
    public static class Dao {

        static final EntityContext ctx = EntityContext.ofDefault();
        static final EntityManager<Employee, Long> empEm = ctx.entityManager(Employee.class, Long.class);
        static final EntityManager<City, Long> cityEm = ctx.entityManager(City.class, Long.class);
        static final ResultSetMapper<EmployeeRelationView> empView = ResultSetMapper.of(EmployeeRelationView.class);

        /** Persists a single entity to the database */
        public void insert(Employee entity, Connection conn) {
            empEm.crud(conn).insert(entity);
        }

        /** Persists a batch of entities */
        public void insertBatch(List<Employee> entities, Connection conn) {
            empEm.crud(conn).insert(entities.stream(), null);
        }

        /** Retrieves all employees */
        public List<Employee> findAllEmployees(Connection conn) {
            return empEm.crud(conn)
                    .selectWhere("", builder -> builder
                            .fetchSize(empEm.defaultBatchSize())
                            .streamMap(empEm.mapper()).toList());
        }

        /** Updates specific columns using batch */
        public void updateSalaryBatch(Stream<Employee> entities, Connection conn) {
            empEm.crud(conn).update(entities, "salary", "updatedAt");
        }

        /** Updates entities with changed snapshot detection */
        public void updateChangedBatch(List<Employee> entities, Connection conn) {
            var array = entities.toArray(new Employee[0]);
            empEm.crud(conn).updateChanged(array);
        }

        /** Retrieves a City from the database */
        public City getCity(Long id, Connection conn) {
            return cityEm.crud(conn).findById(id).orElse(null);
        }

        /** Retrieves employees joined with city and superior */
        public List<EmployeeRelationView> findWithRelations(Connection conn) {
            var sql = """
                SELECT e.id AS ${e.id}
                , e.name    AS ${e.name}
                , c.name    AS ${c.name}
                , s.name    AS ${s.name}
                FROM employee e
                JOIN city c ON e.city_id = c.id 
                LEFT JOIN employee s ON e.superior_id = s.id
            """;
            return SqlQuery.run(conn, query -> query
                    .sql(sql)
                    .label("e.id"  , QEmployeeRelationView.id)
                    .label("e.name", QEmployeeRelationView.name)
                    .label("c.name", QEmployeeRelationView.cityName)
                    .label("s.name", QEmployeeRelationView.superiorName)
                    .fetchSize(empEm.defaultBatchSize())
                    .streamMap(empView.mapper())
                    .toList());
        }

        /** Retrieves full entities mapped into an object graph from a single query */
        public List<Employee> findEntitiesWithRelations(Connection conn) {
            return SelectQuery.run(conn, empEm, query -> query
                    .sql("SELECT")
                    .columnsOfDomain(false)
                    // --- CITY RELATION ---
                    .column(QEmployee.city, QCity.id)
                    .column(QEmployee.city, QCity.name)
                    .column(QEmployee.city, QCity.countryCode)
                    .column(QEmployee.city, QCity.latitude)
                    .column(QEmployee.city, QCity.longitude)
                    .column(QEmployee.city, QCity.createdAt)
                    .column(QEmployee.city, QCity.createdBy)
                    .column(QEmployee.city, QCity.updatedAt)
                    .column(QEmployee.city, QCity.updatedBy)
                    // --- SUPERIOR RELATION ---
                    .column(QEmployee.superior, QEmployee.id)
                    .column(QEmployee.superior, QEmployee.name)
                    .column(QEmployee.superior, QEmployee.contractDay)
                    .column(QEmployee.superior, QEmployee.department)
                    .column(QEmployee.superior, QEmployee.email)
                    .column(QEmployee.superior, QEmployee.isActive)
                    .column(QEmployee.superior, QEmployee.phone)
                    .column(QEmployee.superior, QEmployee.salary)
                    .column(QEmployee.superior, QEmployee.createdAt)
                    .column(QEmployee.superior, QEmployee.createdBy)
                    .column(QEmployee.superior, QEmployee.updatedAt)
                    .column(QEmployee.superior, QEmployee.updatedBy)
                    // --- EXECUTION ---
                    .fetchSize(empEm.defaultBatchSize())
                    .toList());
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
        service.executeInTransaction((dao, conn) -> {
            var city = dao.getCity(1L, conn);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    dao.insert(employee, conn);
                }
            });
        });
    }

    /** Executes a batch insert test */
    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInTransaction((dao, conn) -> {
            var city = dao.getCity(1L, conn);
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
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
        });
    }

    /** Executes updates on selected columns */
    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInTransaction((dao, conn) -> {
            var employees = dao.findAllEmployees(conn);
            stopwatch.benchmark(() -> {
                var batch = new ArrayList<Employee>(50);
                for (var employee : employees) {
                    employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                    employee.setUpdatedAt(LocalDateTime.now());
                    batch.add(employee);
                    if (batch.size() == 50) {
                        dao.updateSalaryBatch(batch.stream(), conn);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    dao.updateSalaryBatch(batch.stream(), conn);
                }
            });
        });
    }

    /** Executes updates on randomly modified columns */
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInTransaction((dao, conn) -> {
            var employees = dao.findAllEmployees(conn);
            stopwatch.benchmark(() -> {
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
        });
    }

    /** Reads data including mapped relations */
    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeReadOnly((dao, conn) -> {
            stopwatch.benchmark(() -> {
                var result = dao.findWithRelations(conn);
            });
        });
    }

    /** Reads full entities including mapped relations */
    @Override
    public void testReadRelatedEntities(Stopwatch stopwatch) {
        service.executeReadOnly((dao, conn) -> {
            stopwatch.benchmark(() -> {
                var result = dao.findEntitiesWithRelations(conn);
            });
        });
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
