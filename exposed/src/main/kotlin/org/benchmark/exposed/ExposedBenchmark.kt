package org.benchmark.exposed

import org.benchmark.common.DatabaseUtils
import org.benchmark.common.EmployeeRelationView
import org.benchmark.common.OrmBenchmark
import org.benchmark.common.Stopwatch
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.date
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import kotlin.random.Random

/** Main benchmark class for Exposed */
class ExposedBenchmark : OrmBenchmark {

    /** City table mapping */
    object Cities : Table("CITY") {
        val id = long("ID").autoIncrement()
        val name = varchar("NAME", 50)
        val countryCode = varchar("COUNTRY_CODE", 2)
        val latitude = decimal("LATITUDE", 10, 8)
        val longitude = decimal("LONGITUDE", 11, 8)
        val createdAt = datetime("CREATED_AT")
        val updatedAt = datetime("UPDATED_AT")
        val createdBy = varchar("CREATED_BY", 50)
        val updatedBy = varchar("UPDATED_BY", 50)

        override val primaryKey = PrimaryKey(id)
    }

    /** Employee table mapping */
    object Employees : Table("EMPLOYEE") {
        val id = long("ID").autoIncrement()
        val name = varchar("NAME", 50)
        val superiorId = long("SUPERIOR_ID").references(id).nullable()
        val cityId = long("CITY_ID").references(Cities.id)
        val contractDay = date("CONTRACT_DAY").nullable()
        val isActive = bool("IS_ACTIVE").default(true)
        val email = varchar("EMAIL", 100).nullable()
        val phone = varchar("PHONE", 20).nullable()
        val salary = decimal("SALARY", 10, 2).nullable()
        val department = varchar("DEPARTMENT", 50).nullable()
        val createdAt = datetime("CREATED_AT")
        val updatedAt = datetime("UPDATED_AT")
        val createdBy = varchar("CREATED_BY", 50)
        val updatedBy = varchar("UPDATED_BY", 50)

        override val primaryKey = PrimaryKey(id)
    }

    /** Employee data transfer object */
    data class Employee(
        var id: Long? = null,
        var name: String,
        var cityId: Long,
        var contractDay: LocalDate?,
        var isActive: Boolean,
        var email: String?,
        var phone: String?,
        var salary: BigDecimal?,
        var department: String?,
        var createdAt: LocalDateTime,
        var updatedAt: LocalDateTime,
        var createdBy: String,
        var updatedBy: String
    )

    /** Data Access Object for entities */
    class Dao {
        /** Inserts a single employee */
        fun insert(emp: Employee) {
            Employees.insert {
                it[name] = emp.name
                it[cityId] = emp.cityId
                it[contractDay] = emp.contractDay
                it[isActive] = emp.isActive
                it[email] = emp.email
                it[phone] = emp.phone
                it[salary] = emp.salary
                it[department] = emp.department
                it[createdAt] = emp.createdAt
                it[updatedAt] = emp.updatedAt
                it[createdBy] = emp.createdBy
                it[updatedBy] = emp.updatedBy
            }
        }

        /** Inserts a batch of employees */
        fun insertBatch(employees: List<Employee>) {
            Employees.batchInsert(employees) { emp ->
                this[Employees.name] = emp.name
                this[Employees.cityId] = emp.cityId
                this[Employees.contractDay] = emp.contractDay
                this[Employees.isActive] = emp.isActive
                this[Employees.email] = emp.email
                this[Employees.phone] = emp.phone
                this[Employees.salary] = emp.salary
                this[Employees.department] = emp.department
                this[Employees.createdAt] = emp.createdAt
                this[Employees.updatedAt] = emp.updatedAt
                this[Employees.createdBy] = emp.createdBy
                this[Employees.updatedBy] = emp.updatedBy
            }
        }

        /** Retrieves all employees */
        fun findAllEmployees(): List<Employee> {
            return Employees.selectAll().map {
                Employee(
                    id = it[Employees.id],
                    name = it[Employees.name],
                    cityId = it[Employees.cityId],
                    contractDay = it[Employees.contractDay],
                    isActive = it[Employees.isActive],
                    email = it[Employees.email],
                    phone = it[Employees.phone],
                    salary = it[Employees.salary],
                    department = it[Employees.department],
                    createdAt = it[Employees.createdAt],
                    updatedAt = it[Employees.updatedAt],
                    createdBy = it[Employees.createdBy],
                    updatedBy = it[Employees.updatedBy]
                )
            }
        }

        /** Updates an employee salary */
        fun updateSalary(empId: Long, newSalary: BigDecimal) {
            Employees.update({ Employees.id eq empId }) {
                it[salary] = newSalary
                it[updatedAt] = LocalDateTime.now()
            }
        }

        /** Updates an employee active state */
        fun updateActiveState(empId: Long, newActive: Boolean) {
            Employees.update({ Employees.id eq empId }) {
                it[isActive] = newActive
                it[updatedAt] = LocalDateTime.now()
            }
        }

        /** Updates an employee department */
        fun updateDepartment(empId: Long, newDept: String) {
            Employees.update({ Employees.id eq empId }) {
                it[department] = newDept
                it[updatedAt] = LocalDateTime.now()
            }
        }

        /** Retrieves employees with relations */
        fun findWithRelations(): List<EmployeeRelationView> {
            val superiorAlias = Employees.alias("superior")
            val superiorNameAlias = superiorAlias[Employees.name]

            return Employees
                .innerJoin(Cities)
                .leftJoin(
                    otherTable = superiorAlias,
                    onColumn = { Employees.superiorId },
                    otherColumn = { superiorAlias[Employees.id] }
                )
                .selectAll()
                .map {
                    EmployeeRelationView(
                        it[Employees.id],
                        it[Employees.name],
                        it[Cities.name],
                        it[superiorNameAlias]
                    )
                }
        }
    }

    /** Service layer managing transactions */
    class Service {
        init {
            Database.connect({ DatabaseUtils.getConnection() })
        }

        /** Executes the given action inside a managed database transaction */
        fun executeInTransaction(action: (Dao) -> Unit) {
            transaction {
                action(Dao())
            }
        }

        /** Executes the given action in a read-only transaction */
        fun executeReadOnly(action: (Dao) -> Unit) {
            transaction {
                connection.readOnly = true
                action(Dao())
            }
        }
    }

    private val service = Service()

    /** Executes a single row insert test */
    fun testSingleInsert(stopwatch: Stopwatch) {
        service.executeInTransaction { dao ->
            stopwatch.benchmark {
                for (i in 1..stopwatch.iterations) {
                    val employee = createRandomEmployee()
                    dao.insert(employee)
                }
            }
        }
    }

    /** Executes a batch insert test */
    override fun testBatchInsert(stopwatch: Stopwatch) {
        service.executeInTransaction { dao ->
            stopwatch.benchmark {
                val batch = mutableListOf<Employee>()
                for (i in 1..stopwatch.iterations) {
                    batch.add(createRandomEmployee())
                    if (i % 50 == 0) {
                        dao.insertBatch(batch)
                        batch.clear()
                    }
                }
                if (batch.isNotEmpty()) {
                    dao.insertBatch(batch)
                }
            }
        }
    }

    /** Executes updates on selected columns */
    override fun testSpecificUpdate(stopwatch: Stopwatch) {
        service.executeInTransaction { dao ->
            val employees = dao.findAllEmployees()
            stopwatch.benchmark {
                for (employee in employees) {
                    val newSalary = (employee.salary ?: BigDecimal.ZERO).add(BigDecimal.valueOf(1000))
                    dao.updateSalary(employee.id!!, newSalary)
                }
            }
        }
    }

    /** Executes updates on randomly modified columns */
    override fun testRandomUpdate(stopwatch: Stopwatch) {
        val random = Random.Default
        service.executeInTransaction { dao ->
            val employees = dao.findAllEmployees()
            stopwatch.benchmark {
                for (employee in employees) {
                    if (random.nextBoolean()) {
                        dao.updateActiveState(employee.id!!, !employee.isActive)
                    } else {
                        dao.updateDepartment(employee.id!!, "Dept-" + random.nextInt(100))
                    }
                }
            }
        }
    }

    /** Reads data including mapped relations */
    override fun testReadWithRelations(stopwatch: Stopwatch) {
        service.executeReadOnly { dao ->
            stopwatch.benchmark {
                val result = dao.findWithRelations()
            }
        }
    }

    companion object {
        /** Creates a random employee instance */
        fun createRandomEmployee(): Employee {
            return Employee(
                name = "Name",
                cityId = 1L,
                contractDay = LocalDate.now(),
                isActive = true,
                email = "test@example.com",
                phone = "123456789",
                salary = BigDecimal.valueOf(50000),
                department = "IT",
                createdAt = LocalDateTime.now(),
                updatedAt = LocalDateTime.now(),
                createdBy = "System",
                updatedBy = "System"
            )
        }
    }
}