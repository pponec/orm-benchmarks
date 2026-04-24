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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

/** Main benchmark class for Avaje Ebean. */
public class EbeanBenchmark implements OrmBenchmark, AutoCloseable {

    private final Database database;

    /** Initialize the H2 database schema and Ebean instance. */
    public EbeanBenchmark() {
        initializeSchema();
        preloadPoolStatusClass();

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

    /** Ensures schema is initialized in the in-memory database. */
    private static void initializeSchema() {
        try (var connection = DatabaseUtils.getConnection()) {
            // DatabaseUtils performs the DDL and keeps it in memory
        } catch (java.sql.SQLException e) {
            throw new RuntimeException("Failed to initialize database via DatabaseUtils", e);
        }
    }

    /** Preloads Ebean pool status class to avoid shutdown classloading issues. */
    private static void preloadPoolStatusClass() {
        try {
            Class.forName("io.ebean.datasource.pool.ConnectionPool$Status");
        } catch (ClassNotFoundException e) {
            // Class might be missing in some versions
        }
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
    public int testSingleInsert(Stopwatch stopwatch) {
        var city = database.find(City.class, 1L);
        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    database.save(employee);
                }
                transaction.commit();
            }
        });
        return stopwatch.getIterations();
    }

    /** Execute a batch insert test using transaction batching. */
    @Override
    public int testBatchInsert(Stopwatch stopwatch) {
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
        return stopwatch.getIterations();
    }

    /**
     * Execute updates on specific columns using Ebean dirty checking.
     * This keeps updates type-safe and uses a framework-native ORM workflow.
     */
    @Override
    public int testSpecificUpdate(Stopwatch stopwatch) {
        var employees = database.find(Employee.class).findList();
        stopwatch.benchmark(() -> {
            try (var transaction = database.beginTransaction()) {
                transaction.setBatchSize(50);
                for (var e : employees) {
                    e.setSalary(e.getSalary().add(BigDecimal.valueOf(1000)));
                    e.setUpdatedAt(LocalDateTime.now());
                    database.save(e);
                }
                transaction.commit();
            }
        });
        return employees.size();
    }

    /** Execute updates using automatic dirty checking. */
    @Override
    public int testRandomUpdate(Stopwatch stopwatch) {
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
        return employees.size();
    }

    /** Read data into a DTO projection. */
    @Override
    public List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch) {
        var result = new AtomicReference<List<EmployeeRelationView>>(List.of());
        stopwatch.benchmark(() -> {
            result.set(database.findDto(EmployeeRelationView.class, """
                    select e.id, e.name, c.name as cityName, s.name as superiorName
                    from employee e
                    join city c on c.id = e.city_id
                    left join employee s on s.id = e.superior_id
                    """)
                    .findList());
        });
        return result.get();
    }

//    /** Throws NPE (?) */
//    @Override
//    public void testReadWithRelations_NPE(Stopwatch stopwatch) {
//        var e = QEmployee.alias();
//
//        stopwatch.benchmark(() -> new QEmployee()
//                .select(e.id, e.name, e.city.name, e.superior.name, e.superior.superior.superior.superior.name)
//                .asDto(EmployeeRelationView.class)
//                .findList()
//        );
//    }

    /**
     * Read entities with relations using Ebean entity query + fetch graph.
     * This reflects the canonical ORM-style usage in Ebean.
     */
    @Override
    public List<Employee> testReadRelatedEntities(Stopwatch stopwatch) {
        var result = new AtomicReference<List<Employee>>(List.of());
        stopwatch.benchmark(() -> result.set(database.find(Employee.class)
                .fetch("city")
                .fetch("superior")
                .findList()));
        return result.get();
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