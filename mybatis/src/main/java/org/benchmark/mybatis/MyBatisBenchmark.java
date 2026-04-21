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
import java.util.function.Consumer;

/** Main benchmark class for MyBatis */
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

    /** Rich Domain Object for Reads */
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
        void insert(Employee employee);

        // Opraveno: Načtení do Listu namísto použití Cursoru pro férové načtení dat do paměti před updaty
        @Select("""
            SELECT * FROM employee
            """)
        List<Employee> getAllEmployees();

        @Update("""
            UPDATE employee
            SET salary = #{salary}, updated_at = #{updatedAt}
            WHERE id = #{id}
            """)
        void updateSpecific(Employee employee);

        @Update("""
            UPDATE employee
            SET is_active = #{isActive}, department = #{department}, updated_at = #{updatedAt}
            WHERE id = #{id}
            """)
        void updateRandom(Employee employee);

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
                 , c.id AS "city.id"
                 , c.name AS "city.name"
                 , c.country_code AS "city.countryCode"
                 , c.latitude AS "city.latitude"
                 , c.longitude AS "city.longitude"
                 , c.created_at AS "city.createdAt"
                 , c.updated_at AS "city.updatedAt"
                 , c.created_by AS "city.createdBy"
                 , c.updated_by AS "city.updatedBy"
                 , s.id AS "superior.id"
                 , s.name AS "superior.name"
            FROM employee e
            JOIN city c ON e.city_id = c.id
            LEFT JOIN employee s ON e.superior_id = s.id
            """)
        List<RichEmployee> getEntitiesWithRelations();
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
    public void testSingleInsert(Stopwatch stopwatch) {
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var city = mapper.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    mapper.insert(createRandomEmployee(city));
                }
            });
        });
    }

    @Override
    public void testBatchInsert(Stopwatch stopwatch) {
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var city = mapper.getCity(1L);
            stopwatch.benchmark(() -> {
                for (var i = 1; i <= stopwatch.getIterations(); i++) {
                    mapper.insert(createRandomEmployee(city));
                    if (i % 50 == 0) {
                        session.flushStatements();
                    }
                }
                session.flushStatements();
            });
        });
    }

    @Override
    public void testSpecificUpdate(Stopwatch stopwatch) {
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            // Férové načtení dat do paměti PŘED stiskem stopek
            var employees = mapper.getAllEmployees();

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
    }

    @Override
    public void testRandomUpdate(Stopwatch stopwatch) {
        var random = new Random();
        service.executeInBatchTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            var employees = mapper.getAllEmployees();

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
    }

    @Override
    public void testReadWithRelations(Stopwatch stopwatch) {
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            stopwatch.benchmark(() -> {
                var result = mapper.getRelations();
            });
        });
    }

    @Override
    public void testReadRelatedEntities(Stopwatch stopwatch) {
        service.executeInTransaction(session -> {
            var mapper = session.getMapper(EmployeeMapper.class);
            stopwatch.benchmark(() -> {
                var result = mapper.getEntitiesWithRelations();
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