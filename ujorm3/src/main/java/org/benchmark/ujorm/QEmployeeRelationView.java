package org.benchmark.ujorm;

import javax.annotation.processing.Generated;

import org.benchmark.common.EmployeeRelationView;
import org.ujorm.core.Key;
import org.ujorm.core.DomainHandler;
import org.ujorm.core.DomainHandlerProvider;
/** Auto-generated metamodel for the {@code EmployeeRelationView} domain class. */
//@Generated("org.ujorm.maven.UjormMetaProcessor.SourceGenerator")
public abstract class QEmployeeRelationView {

    private static final DomainHandler<EmployeeRelationView> meta = DomainHandlerProvider.getHandler(EmployeeRelationView.class);

    /** The {@code cityName} property descriptor */
    public static final Key<EmployeeRelationView, String> cityName = meta.getKey("cityName");
    /** The {@code id} property descriptor */
    public static final Key<EmployeeRelationView, Long> id = meta.getKey("id");
    /** The {@code name} property descriptor */
    public static final Key<EmployeeRelationView, String> name = meta.getKey("name");
    /** The {@code superiorName} property descriptor */
    public static final Key<EmployeeRelationView, String> superiorName = meta.getKey("superiorName");
}
