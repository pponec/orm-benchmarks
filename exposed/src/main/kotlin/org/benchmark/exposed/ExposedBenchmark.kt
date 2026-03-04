package org.benchmark.exposed

import org.benchmark.common.DatabaseUtils
import org.benchmark.common.EmployeeRelationView
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
class ExposedBenchmark {

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
                it[Employees.name] = emp.name
                it[Employees.cityId] = emp.cityId
                it[Employees.contractDay] = emp.contractDay
                it[Employees.isActive] = emp.isActive
                it[Employees.email] = emp.email
                it[Employees.phone] = emp.phone
                it[Employees.salary] = emp.salary
                it[Employees.department] = emp.department
                it[Employees.createdAt] = emp.createdAt
                it[Employees.updatedAt] = emp.updatedAt
                it[Employees.createdBy] = emp.createdBy
                it[Employees.updatedBy] = emp.updatedBy
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
            val result = Employees.selectAll().map {
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
            return result
        }

        /** Updates an employee salary */
        fun updateSalary(empId: Long, newSalary: BigDecimal) {
            Employees.update({ Employees.id eq empId }) {
                it[Employees.salary] = newSalary
                it[Employees.updatedAt] = LocalDateTime.now()
            }
        }

        /** Updates an employee active state */
        fun updateActiveState(empId: Long, newActive: Boolean) {
            Employees.update({ Employees.id eq empId }) {
                it[Employees.isActive] = newActive
                it[Employees.updatedAt] = LocalDateTime.now()
            }
        }

        /** Updates an employee department */
        fun updateDepartment(empId: Long, newDept: String) {
            Employees.update({ Employees.id eq empId }) {
                it[Employees.department] = newDept
                it[Employees.updatedAt] = LocalDateTime.now()
            }
        }

        /** Retrieves employees with relations */
        fun findWithRelations(): List<EmployeeRelationView> {
            val superiorAlias = Employees.alias("superior")
            val superiorNameAlias = superiorAlias[Employees.name]

            val result = Employees
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
            return result
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
        stopwatch.start()
        service.executeInTransaction { dao ->
            for (i in 1..100_000) {
                val employee = createRandomEmployee()
                dao.insert(employee)
            }
        }
        stopwatch.stop()
    }

    /** Executes a batch insert test */
    fun testBatchInsert(stopwatch: Stopwatch) {
        stopwatch.start()
        service.executeInTransaction { dao ->
            val batch = mutableListOf<Employee>()
            for (i in 1..100_000) {
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
        stopwatch.stop()
    }

    /** Executes updates on selected columns */
    fun testSpecificUpdate(stopwatch: Stopwatch) {
        stopwatch.start()
        service.executeInTransaction { dao ->
            val employees = dao.findAllEmployees()
            for (employee in employees) {
                val newSalary = (employee.salary ?: BigDecimal.ZERO).add(BigDecimal.valueOf(1000))
                dao.updateSalary(employee.id!!, newSalary)
            }
        }
        stopwatch.stop()
    }

    /** Executes updates on randomly modified columns */
    fun testRandomUpdate(stopwatch: Stopwatch) {
        stopwatch.start()
        val random = Random.Default
        service.executeInTransaction { dao ->
            val employees = dao.findAllEmployees()
            for (employee in employees) {
                if (random.nextBoolean()) {
                    dao.updateActiveState(employee.id!!, !employee.isActive)
                } else {
                    dao.updateDepartment(employee.id!!, "Dept-" + random.nextInt(100))
                }
            }
        }
        stopwatch.stop()
    }

    /** Reads data including mapped relations */
    fun testReadWithRelations(stopwatch: Stopwatch) {
        stopwatch.start()
        service.executeReadOnly { dao ->
            val result = dao.findWithRelations()
        }
        stopwatch.stop()
    }

    companion object {
        /** Creates a random employee instance */
        fun createRandomEmployee(): Employee {
            val result = Employee(
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
            return result
        }
    }
}