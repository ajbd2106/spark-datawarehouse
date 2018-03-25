package com.starschema.dimension.role;

import com.starschema.annotations.dimensions.*;
import com.starschema.annotations.facts.Fact;
import com.starschema.annotations.facts.FactJunkDimension;
import com.starschema.annotations.common.Table;
import com.starschema.columnSelector.AnnotatedJunkDimensionColumnSelector;
import com.starschema.columnSelector.JunkDimensionColumnSelector;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.lookup.AbstractLookup;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
public class JunkDimensionRole extends AbstractDimensionRole {
    private final FactJunkDimension factJunkDimension;
    private final JunkDimensionColumnSelector junkDimensionColumnSelector;

    public JunkDimensionRole(Field field) {
        super(field);
        this.factJunkDimension = field.getAnnotation(FactJunkDimension.class);
        this.junkDimensionColumnSelector = new AnnotatedJunkDimensionColumnSelector(field.getType());

    }

    /***
     * @return get lookupType of junk dimension. flag getFromMasterTable is no taken into account in this implementation
     * @throws RuntimeException
     */
    @Override
    public Class<? extends AbstractLookup> getFactLookupClass(boolean getFromMasterTable) {

        Class<?> junkDimensionClass = field.getType();

        if (IJunkDimension.class.isAssignableFrom(junkDimensionClass)) {
            return junkDimensionClass.getAnnotation(Table.class).lookupType();
        }

        throw new RuntimeException(String.format("lookupType %s declared in fact table for field name %s must be an instance of IJunkDimension", junkDimensionClass.getCanonicalName(), field.getName()));
    }

    /***
     * @return get Checksum of IJunkDimension instance
     */
    @Override
    public Column getFunctionalId(String alias) {
        String role = field.getName();

        String columnAlias = role;
        if(StringUtils.isNotEmpty(alias)){
            columnAlias = String.format("%s.%s", alias, columnAlias);
        }
        return junkDimensionColumnSelector.calculateCheckSumColumn(Fact.class, CheckSum.class, columnAlias).get();
    }


    /***
     * @return get technical id field
     */
    @Override
    public String getTechnicalIdFieldName() {
        return factJunkDimension.technicalId();
    }

    @Override
    public Class<?> getMasterTableClass(){
        return  field.getType();
    }

    @Override
    protected List<Class<? extends Annotation>> getMasterTableFlattenedAnnotations() {
        return Arrays.asList(
                TechnicalId.class, Fact.class
        );
    }

    @Override
    protected List<RoleColumn> getDimensionCaseColumns(List<Field> fields) {
        return fields.stream().map(
                field -> {
                    Object defaultValue = null;

                    TechnicalId technicalId = field.getAnnotation(TechnicalId.class);
                    Fact factValue = field.getAnnotation(Fact.class);

                    if (technicalId != null) {
                        defaultValue = technicalId.defaultValue();
                    } else if (factValue != null) {
                        defaultValue = factValue.defaultValue();
                    }

                    return new RoleColumn(field.getName(), getAlias(), defaultValue);
                }
        ).collect(Collectors.toList());
    }

}
