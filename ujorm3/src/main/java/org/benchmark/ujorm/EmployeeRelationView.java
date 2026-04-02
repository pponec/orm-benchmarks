package org.benchmark.ujorm;

import jakarta.persistence.Table;

/** Data transfer object for employee relations (copy due meta generator) */
@Table
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