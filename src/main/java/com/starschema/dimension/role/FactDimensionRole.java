package com.starschema.dimension.role;

import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.SCD1;
import com.starschema.annotations.dimensions.SCD2;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.facts.FactDimension;
import com.starschema.annotations.general.Table;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class FactDimensionRole extends AbstractDimensionRole {

    private final FactDimension factDimension;

    public FactDimensionRole(Field field) {
        super(field);
        this.factDimension = field.getAnnotation(FactDimension.class);
    }

    @Override
    public Class<?> getFactLookupClass(boolean getFromMasterTable) {

        if (getFromMasterTable) {
            return getMasterTableClass();
        }

        //return lookup type if we don't get from master table
        return factDimension.lookupType().getAnnotation(Table.class).lookupType();
    }

    @Override
    public Column getFunctionalId(String alias) {

        String columnName = field.getName();
        if(StringUtils.isNotEmpty(alias)){
            columnName = String.format("%s.%s", alias, columnName);
        }
        return functions.col(columnName);
    }

    @Override
    public String getTechnicalIdFieldName() {
        return factDimension.technicalId();
    }

    @Override
    public Class<?> getMasterTableClass() {
        return factDimension.lookupType();
    }

    @Override
    protected List<Class<? extends Annotation>> getMasterTableFlattenedAnnotations() {
        return Arrays.asList(
                TechnicalId.class, FunctionalId.class, SCD1.class, SCD2.class
        );
    }

    @Override
    protected List<RoleColumn> getDimensionCaseColumns(List<Field> fields) {
        return fields.stream().map(
                field -> {
                    Object defaultValue = null;
                    TechnicalId technicalId = field.getAnnotation(TechnicalId.class);
                    FunctionalId functionalId = field.getAnnotation(FunctionalId.class);
                    SCD1 scd1 = field.getAnnotation(SCD1.class);
                    SCD2 scd2 = field.getAnnotation(SCD2.class);
                    if (technicalId != null) {
                        defaultValue = technicalId.defaultValue();
                    } else if (functionalId != null) {
                        defaultValue = functionalId.defaultValue();
                    } else if (scd1 != null) {
                        defaultValue = scd1.defaultValue();
                    } else if (scd2 != null) {
                        defaultValue = scd2.defaultValue();
                    }

                    return new RoleColumn(field.getName(), factDimension.roleName(),  defaultValue);
                }
        ).collect(Collectors.toList());
    }

}
