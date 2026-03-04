package org.benchmark.common;

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