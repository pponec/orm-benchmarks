package org.benchmark.ebean;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.OrmBenchmark;
import org.benchmark.common.Stopwatch;
import org.benchmark.ebean.entity.City;
import org.benchmark.ebean.entity.Employee;
import org.benchmark.ebean.entity.query.QEmployee;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Random;

/** Main benchmark class for Avaje Ebean. */
public class EbeanBenchmark implements OrmBenchmark, AutoCloseable {

    private final Database database;

    /** Initialize the H2 database schema and Ebean instance. */
    public EbeanBenchmark() {
        try (var connection = DatabaseUtils.getConnection()) {
            // DatabaseUtils performs the DDL and keeps it in memory
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Failed to initialize database via DatabaseUtils", e);
        }

        // Force load the Status class to prevent NoClassDefFoundError during Maven shutdown
        try {
            Class.forName("io.ebean.datasource.pool.ConnectionPool$Status");
        } catch (ClassNotFoundException e) {
            // Class might be missing in some versions
        }

        var dsConfig = new DataSourceConfig();
        dsConfig.setUrl("jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1");
        dsConfig.setUsername("sa");
        dsConfig.setPassword("");

        var config = new DatabaseConfig();
        config.setDataSourceConfig(dsConfig);
        config.setDdlGenerate(false);
        config.setDdlRun(false);
        config.setDefaultServer(true);
        config.addClass(City.class);
        config.addClass(Employee.class);

        this.database = DatabaseFactory.create(config);
    }

    /** Shutdown the database instance and its connection pool. */
    @Override
    public void close() {
        if (database != null) {
            database.shutdown();
        }
    }

    /** Execute a single row insert test. */
    @Override
    public void testSingleInsert(Stopwatch stopwatch) {
        var city = database.find(City.class, 1L);
        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    database.save(createRandomEmployee(city));
                }
                transaction.commit();
            }
        });
    }

    /** Execute a batch insert test using transaction batching. */
    @Override
    public void testBatchInsert(Stopwatch stopwatch) {
        var city = database.find(City.class, 1L);
        stopwatch.benchmark(() -> {
            var batch = new ArrayList<Employee>(50);
            try (var transaction = database.beginTransaction()) {
                transaction.setBatchSize(50);
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    batch.add(createRandomEmployee(city));
                    if (i % 50 == 0) {
                        database.saveAll(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    database.saveAll(batch);
                }
                transaction.commit();
            }
        });
    }

    /** Execute updates on specific columns using stateless UpdateQuery. */
    @Override
    public void testSpecificUpdate(Stopwatch stopwatch) {
        var employees = database.find(Employee.class).findList();
        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                transaction.setBatchSize(50);

                for (var e : employees) {
                    database.update(Employee.class)
                            .set("salary", e.getSalary().add(BigDecimal.valueOf(1000)))
                            .set("updatedAt", LocalDateTime.now())
                            .where().idEq(e.getId())
                            .update();
                }
                transaction.commit();
            }
        });
    }

    /** Throws NPE */
    // @Override
    public void testSpecificUpdate_NPE(Stopwatch stopwatch) {
        var employees = database.find(Employee.class).findList();
        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                transaction.setBatchSize(50);
                var eAlias = QEmployee.alias();

                for (var e : employees) {
                    new QEmployee()
                            .id.equalTo(e.getId())
                            .asUpdate()
                            .set(eAlias.salary, e.getSalary().add(BigDecimal.valueOf(1000)))
                            .set(eAlias.updatedAt, LocalDateTime.now())
                            .update();
                }
                transaction.commit();
            }
        });
    }

    /** Execute updates using automatic dirty checking. */
    @Override
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        var employees = database.find(Employee.class).findList();

        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                transaction.setBatchSize(50);
                for (var e : employees) {
                    if (random.nextBoolean()) {
                        e.setIsActive(!e.getIsActive());
                    } else {
                        e.setDepartment("Dept-" + random.nextInt(100));
                    }
                    e.setUpdatedAt(LocalDateTime.now());
                    database.save(e);
                }
                transaction.commit();
            }
        });
    }

    /** Read data into a DTO projection. */
    @Override
    public void testReadWithRelations(Stopwatch stopwatch) {
        stopwatch.benchmark(() -> {
            var result = database.findDto(EmployeeRelationView.class, """
                    select e.id, e.name, c.name as cityName, s.name as superiorName
                    from employee e
                    join city c on c.id = e.city_id
                    left join employee s on s.id = e.superior_id
                    """)
                    .findList();
        });
    }

    /** Throws NPE */
    // @Override
    public void testReadWithRelations_NPE(Stopwatch stopwatch) {
        var e = QEmployee.alias();

        stopwatch.benchmark(() -> new QEmployee()
                .select(e.id, e.name, e.city.name, e.superior.name, e.superior.superior.superior.superior.name)
                .asDto(EmployeeRelationView.class)
                .findList()
        );
    }

    /** Read full entities including mapped relations using fetch joins. */
    @Override
    public void testReadRelatedEntities(Stopwatch stopwatch) {
        stopwatch.benchmark(() -> database.find(Employee.class)
                .fetch("city")
                .fetch("superior")
                .findList());
    }

    /** Create a random employee instance for testing. */
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