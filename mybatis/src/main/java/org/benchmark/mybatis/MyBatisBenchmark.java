package org.benchmark.mybatis;

import lombok.Getter;
import lombok.Setter;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.benchmark.common.DatabaseUtils;
import org.benchmark.common.EmployeeRelationView;
import org.benchmark.common.Stopwatch;
import org.benchmark.common.OrmBenchmark;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Main benchmark class for MyBatis.
 * Uses mapper interfaces with annotation-based SQL, which is a typical MyBatis approach.
 */
public class MyBatisBenchmark implements OrmBenchmark {

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
    public static class EmployeeFlat {
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

    /** Rich Domain Object for Reads */
    @Getter @Setter
    public static class Employee {
        private Long id;
        private String name;
        private City city;
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

    /** Flat row used for manual relation mapping in measured time */
    @Getter @Setter
    public static class RichEmployeeRow {
        private Long id;
        private String name;
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

        private Long cityId;
        private String cityName;
        private String cityCountryCode;
        private BigDecimal cityLatitude;
        private BigDecimal cityLongitude;
        private LocalDateTime cityCreatedAt;
        private LocalDateTime cityUpdatedAt;
        private String cityCreatedBy;
        private String cityUpdatedBy;

        private Long superiorId;
        private String superiorName;
        private LocalDate superiorContractDay;
        private Boolean superiorIsActive;
        private String superiorEmail;
        private String superiorPhone;
        private BigDecimal superiorSalary;
        private String superiorDepartment;
        private LocalDateTime superiorCreatedAt;
        private LocalDateTime superiorUpdatedAt;
        private String superiorCreatedBy;
        private String superiorUpdatedBy;
    }

    public interface EmployeeMapper {
        @Select("""
            SELECT * FROM city WHERE id = #{id}
            """)
        City getCity(Long id);

        @Insert("""
            INSERT INTO employee (
                name, superior_id, city_id, contract_day, is_active, email, phone, salary, department, created_at, updated_at, created_by, updated_by
            ) VALUES (
                #{name}, #{superiorId}, #{cityId}, #{contractDay}, #{isActive}, #{email}, #{phone}, #{salary}, #{department}, #{createdAt}, #{updatedAt}, #{createdBy}, #{updatedBy}
            )
            """)
        @Options(useGeneratedKeys = true, keyProperty = "id")
        void insert(EmployeeFlat employee);

        // Opraveno: Načtení do Listu namísto použití Cursoru pro férové načtení dat do paměti před updaty
        @Select("""
            SELECT * FROM employee
            """)
        List<EmployeeFlat> getAllEmployees();

        @Update("""
            UPDATE employee
            SET salary = #{salary}, updated_at = #{updatedAt}
            WHERE id = #{id}
            """)
        void updateSpecific(EmployeeFlat employee);

        @Update("""
            UPDATE employee
            SET is_active = #{isActive}, department = #{department}, updated_at = #{updatedAt}
            WHERE id = #{id}
            """)
        void updateRandom(EmployeeFlat employee);

        @Select("""
            SELECT e.id, e.name, c.name AS cityName, s.name AS superiorName
            FROM employee e
            JOIN city c ON e.city_id = c.id
            LEFT JOIN employee s ON e.superior_id = s.id
            """)
        List<EmployeeRelationView> getRelations();

        @Select("""
            SELECT e.id
                 , e.name
                 , e.contract_day AS contractDay
                 , e.is_active AS isActive
                 , e.email
                 , e.phone
                 , e.salary
                 , e.department
                 , e.created_at AS createdAt
                 , e.updated_at AS updatedAt
                 , e.created_by AS createdBy
                 , e.updated_by AS updatedBy
                 , c.id AS cityId
                 , c.name AS cityName
                 , c.country_code AS cityCountryCode
                 , c.latitude AS cityLatitude
                 , c.longitude AS cityLongitude
                 , c.created_at AS cityCreatedAt
                 , c.updated_at AS cityUpdatedAt
                 , c.created_by AS cityCreatedBy
                 , c.updated_by AS cityUpdatedBy
                 , s.id AS superiorId
                 , s.name AS superiorName
                 , s.contract_day AS superiorContractDay
                 , s.is_active AS superiorIsActive
                 , s.email AS superiorEmail
                 , s.phone AS superiorPhone
                 , s.salary AS superiorSalary
                 , s.department AS superiorDepartment
                 , s.created_at AS superiorCreatedAt
                 , s.updated_at AS superiorUpdatedAt
                 , s.created_by AS superiorCreatedBy
                 , s.updated_by AS superiorUpdatedBy
            FROM employee e
            JOIN city c ON e.city_id = c.id
            LEFT JOIN employee s ON e.superior_id = s.id
            """)
        List<RichEmployeeRow> getEntitiesWithRelationsRaw();
    }

    public static class Service {
        private final SqlSessionFactory sessionFactory;

        public Service() {
            var dataSource = new UnpooledDataSource("org.h2.Driver", "jdbc:h2:mem:benchmark;DB_CLOSE_DELAY=-1", "sa", "");
            var environment = new Environment("benchmark", new JdbcTransactionFactory(), dataSource);
            var configuration = new Configuration(environment);

            configuration.setMapUnderscoreToCamelCase(true);
            configuration.addMapper(EmployeeMapper.class);
            configuration.setDefaultExecutorType(ExecutorType.SIMPLE);

            this.sessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        }

        public void executeInTransaction(Consumer<SqlSession> action) {
            execute(ExecutorType.SIMPLE, action);
        }

        public void executeInBatchTransaction(Consumer<SqlSession> action) {
            execute(ExecutorType.BATCH, action);
        }

        private void execute(ExecutorType executorType, Consumer<SqlSession> action) {
            try (var connection = DatabaseUtils.getConnection();
                 var session = sessionFactory.openSession(executorType, connection)) {
                try {
                    action.accept(session);
                    session.commit();
                } catch (Exception e) {
                    session.rollback();
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private final Service service;

    public MyBatisBenchmark() {
        this.service = new Service();
    }

    @Override
    public int testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var city = mapper.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    mapper.insert(employee);
                }
            });
        });
        return stopwatch.getIterations();
    }

    @Override
    public int testBatchInsert(Stopwatch stopwatch) {
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var city = mapper.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    var employee = createRandomEmployee(city);
                    mapper.insert(employee);
                    if (i % 50 == 0) {
                        session.flushStatements();
                    }
                }
                session.flushStatements();
            });
        });
        return stopwatch.getIterations();
    }

    @Override
    public int testSpecificUpdate(Stopwatch stopwatch) {
        var updatedCount = new AtomicReference<>(0);
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            // Férové načtení dat do paměti PŘED stiskem stopek
            var employees = mapper.getAllEmployees();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var count = 0;
                for (var employee : employees) {
                    employee.setSalary(employee.getSalary().add(BigDecimal.valueOf(1000)));
                    employee.setUpdatedAt(LocalDateTime.now());
                    mapper.updateSpecific(employee);
                    if (++count % 50 == 0) {
                        session.flushStatements();
                        session.clearCache();
                    }
                }
                session.flushStatements();
            });
        });
        return updatedCount.get();
    }

    @Override
    public int testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        var updatedCount = new AtomicReference<>(0);
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var employees = mapper.getAllEmployees();
            updatedCount.set(employees.size());

            stopwatch.benchmark(() -> {
                var count = 0;
                for (var employee : employees) {
                    if (random.nextBoolean()) {
                        employee.setIsActive(!employee.getIsActive());
                    } else {
                        employee.setDepartment("Dept-" + random.nextInt(100));
                    }
                    employee.setUpdatedAt(LocalDateTime.now());
                    mapper.updateRandom(employee);
                    if (++count % 50 == 0) {
                        session.flushStatements();
                        session.clearCache();
                    }
                }
                session.flushStatements();
            });
        });
        return updatedCount.get();
    }

    @Override
    public List<EmployeeRelationView> testReadWithRelations(Stopwatch stopwatch) {
        var result = new AtomicReference<List<EmployeeRelationView>>(List.of());
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            stopwatch.benchmark(() -> {
                result.set(mapper.getRelations());
            });
        });
        return result.get();
    }

    @Override
    public List<Employee> testReadRelatedEntities(Stopwatch stopwatch) {
        var result = new AtomicReference<List<Employee>>(List.of());
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            stopwatch.benchmark(() -> {
                result.set(mapper.getEntitiesWithRelationsRaw().stream().map(row -> {
                    var city = new City();
                    city.setId(row.getCityId());
                    city.setName(row.getCityName());
                    city.setCountryCode(row.getCityCountryCode());
                    city.setLatitude(row.getCityLatitude());
                    city.setLongitude(row.getCityLongitude());
                    city.setCreatedAt(row.getCityCreatedAt());
                    city.setUpdatedAt(row.getCityUpdatedAt());
                    city.setCreatedBy(row.getCityCreatedBy());
                    city.setUpdatedBy(row.getCityUpdatedBy());

                    Employee superior = null;
                    if (row.getSuperiorId() != null) {
                        superior = new Employee();
                        superior.setId(row.getSuperiorId());
                        superior.setName(row.getSuperiorName());
                        superior.setContractDay(row.getSuperiorContractDay());
                        superior.setIsActive(row.getSuperiorIsActive());
                        superior.setEmail(row.getSuperiorEmail());
                        superior.setPhone(row.getSuperiorPhone());
                        superior.setSalary(row.getSuperiorSalary());
                        superior.setDepartment(row.getSuperiorDepartment());
                        superior.setCreatedAt(row.getSuperiorCreatedAt());
                        superior.setUpdatedAt(row.getSuperiorUpdatedAt());
                        superior.setCreatedBy(row.getSuperiorCreatedBy());
                        superior.setUpdatedBy(row.getSuperiorUpdatedBy());
                    }

                    var employee = new Employee();
                    employee.setId(row.getId());
                    employee.setName(row.getName());
                    employee.setCity(city);
                    employee.setSuperior(superior);
                    employee.setContractDay(row.getContractDay());
                    employee.setIsActive(row.getIsActive());
                    employee.setEmail(row.getEmail());
                    employee.setPhone(row.getPhone());
                    employee.setSalary(row.getSalary());
                    employee.setDepartment(row.getDepartment());
                    employee.setCreatedAt(row.getCreatedAt());
                    employee.setUpdatedAt(row.getUpdatedAt());
                    employee.setCreatedBy(row.getCreatedBy());
                    employee.setUpdatedBy(row.getUpdatedBy());
                    return employee;
                }).toList());
            });
        });
        return result.get();
    }

    public static EmployeeFlat createRandomEmployee(City city) {
        var result = new EmployeeFlat();
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