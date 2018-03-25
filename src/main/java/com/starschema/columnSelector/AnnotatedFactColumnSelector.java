package com.starschema.columnSelector;

import com.starschema.annotations.dimensions.CheckSum;
import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.dimensions.UpdatedDate;
import com.starschema.annotations.facts.Fact;
import com.starschema.annotations.facts.FactDimension;
import com.starschema.annotations.facts.FactJunkDimension;
import com.starschema.annotations.facts.InventoryDate;
import com.starschema.dimension.role.FactDimensionRole;
import com.starschema.dimension.role.IDimensionRole;
import com.starschema.dimension.role.JunkDimensionRole;
import com.starschema.fact.IFact;
import com.starschema.lookup.AbstractLookup;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class AnnotatedFactColumnSelector<T extends IFact> extends CommonColumnSelector<T> implements FactColumnSelector {

    public AnnotatedFactColumnSelector(Class<T> targetClass) {
        super(targetClass);
    }

    @Override
    public List<IDimensionRole> getLookupDimension() {
        List<IDimensionRole> dimensionRoles = getDimensionLookupRole();
        dimensionRoles.addAll(getJunkDimensionLookupRole());
        return dimensionRoles;
    }

    @Override
    public Map<Class, List<Field>> getJunkDimensions() {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(targetClass, FactJunkDimension.class);
        Map<Class, List<Field>> groupedJunkDimension = dimensionFields.stream().collect(Collectors.groupingBy(field -> field.getType()));
        return groupedJunkDimension;
    }

    @Override
    public String getLookupFunctionalId(Class<?> lookup, String alias) {
        return getLookupField(lookup, alias, FunctionalId.class);
    }

    @Override
    public String getInventoryDateName() {
        return getInventoryDateName(StringUtils.EMPTY);
    }

    @Override
    public Column[] getFinalColumns(Integer inventoryDate, String inventoryDateFieldName, List<IDimensionRole> factDimensionRoleList, String currentAlias) {

        List<Column> columnList = new ArrayList<>();

        columnList.add(lit(inventoryDate).as(inventoryDateFieldName));

        factDimensionRoleList.forEach(
                factDimension -> {
                    Class<?> lookupClass = factDimension.getFactLookupClass();

                    //get lookup alias + technical Id of lookup table
                    String alias = factDimension.getAlias();
                    String technicalId = factDimension.getTechnicalIdFieldName();
                    String lookupTechnicalId = getLookupTechnicalId(lookupClass, alias);
                    String lookupFunctionalId = getLookupFunctionalId(lookupClass, alias);
                    Long technicalIdFakeValue = getFakeTechnicalIdValue(lookupClass);

                    //if lookup alias + functionalId is null, replace fact functionalId with technical id fake value
                    //if lookup alias + functionalId is not null, replace fact functionalId with alias + technical id value
                    //rename the column as fact table one
                    columnList.add(when(col(lookupFunctionalId).isNull(), lit(technicalIdFakeValue)).otherwise(col(lookupTechnicalId)).as(technicalId));
                }
        );

        columnList.addAll(getColumnsFromAnnotationAsOriginal(Arrays.asList(Fact.class), currentAlias));

        return columnList.toArray(new Column[columnList.size()]);
    }

    @Override
    public Column[] getFlattenedDimensionColumns(List<IDimensionRole> factDimensionRoleList, Integer inventoryDate, String inventoryDateFieldName, String currentAlias) {
        List<Column> columns = new ArrayList<>();
        columns.add(lit(inventoryDate).as(inventoryDateFieldName));
        columns.addAll(getColumnsFromAnnotationAsOriginal(Arrays.asList(Fact.class), currentAlias));
        factDimensionRoleList.forEach(role -> columns.addAll(role.getMasterTableFlattenedFields()));
        return columns.toArray(new Column[columns.size()]);
    }

    private List<IDimensionRole> getDimensionLookupRole() {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(targetClass, FactDimension.class);

        return dimensionFields.stream()
                .map(field -> new FactDimensionRole(field))
                .collect(Collectors.toList());
    }

    private List<IDimensionRole> getJunkDimensionLookupRole() {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(targetClass, FactJunkDimension.class);

        return dimensionFields.stream()
                .map(field -> new JunkDimensionRole(field))
                .collect(Collectors.toList());
    }

    private String getLookupField(Class<?> lookup, String alias, Class<? extends Annotation> annotation) {
        Field finalField = null;
        List<Field> fields = ReflectUtils.getFieldsListWithAnnotation(lookup, annotation);

        for (Field field : fields) {
            if (!field.getDeclaringClass().getSimpleName().equals(AbstractLookup.class.getSimpleName())) {
                finalField = field;
            } else if (finalField == null) {
                finalField = field;
            }
        }

        return String.format("%s.%s", alias, finalField.getName());
    }

    private String getLookupTechnicalId(Class<?> lookup, String alias) {
        return getLookupField(lookup, alias, TechnicalId.class);
    }

    private String getInventoryDateName(String alias) {
        return addAlias(ReflectUtils.getUniqueField(InventoryDate.class, targetClass).get().getName(), alias);
    }

    @Override
    public Column[] removeAliasFromColumns(String alias, Class targetClass) {
        List<Column> allColumns = getColumnsFromAnnotationAsOriginal(
                Arrays.asList(TechnicalId.class, Fact.class, UpdatedDate.class, CheckSum.class), alias, targetClass);

        return allColumns.toArray(new Column[allColumns.size()]);
    }


}
