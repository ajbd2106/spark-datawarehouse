package com.starschema.columnSelector;

import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.TechnicalId;
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

public class FactColumnSelector extends CommonColumnSelector {

    public static List<IDimensionRole> getLookupDimension(Class<? extends IFact> factClass) {
        List<IDimensionRole> dimensionRoles = getDimensionLookupRole(factClass);
        dimensionRoles.addAll(getJunkDimensionLookupRole(factClass));
        return dimensionRoles;
    }

    private static List<IDimensionRole> getDimensionLookupRole(Class<? extends IFact> factClass) {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(factClass, FactDimension.class);

        return dimensionFields.stream()
                .map(field -> new FactDimensionRole(field))
                .collect(Collectors.toList());
    }

    private static List<IDimensionRole> getJunkDimensionLookupRole(Class<? extends IFact> factClass) {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(factClass, FactJunkDimension.class);

        return dimensionFields.stream()
                .map(field -> new JunkDimensionRole(field))
                .collect(Collectors.toList());
    }

    public static Map<Class, List<Field>> getJunkDimensions(Class<? extends IFact> fact) {
        List<Field> dimensionFields = ReflectUtils.getFieldsListWithAnnotation(fact, FactJunkDimension.class);
        Map<Class, List<Field>> groupedJunkDimension = dimensionFields.stream().collect(Collectors.groupingBy(field -> field.getType()));
        return groupedJunkDimension;
    }

    private static String getLookupField(Class<?> lookup, String alias, Class<? extends Annotation> annotation) {
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

    public static String getLookupFunctionalId(Class<?> lookup, String alias) {
        return getLookupField(lookup, alias, FunctionalId.class);
    }

    public static  String getLookupTechnicalId(Class<?> lookup, String alias) {
        return getLookupField(lookup, alias, TechnicalId.class);
    }

    public static String getInventoryDateName(Class<?> className) {
        return getInventoryDateName(StringUtils.EMPTY, className);
    }

    public static String getInventoryDateName(String alias, Class<?> className) {
        return addAlias(ReflectUtils.getUniqueField(InventoryDate.class, className).get().getName(), alias);
    }

    public static Column[] getFinalColumns(Integer inventoryDate, String inventoryDateFieldName, List<IDimensionRole> factDimensionRoleList, Class<? extends IFact> factClass) {

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

        columnList.addAll(getColumnsFromAnnotationAsOriginal(Arrays.asList(Fact.class), CommonColumnSelector.ALIAS_CURRENT, factClass));

        return columnList.toArray(new Column[columnList.size()]);
    }

    public static Column[] getFlattenedDimensionColumns(Class<? extends IFact> factTable,  List<IDimensionRole> factDimensionRoleList, Integer inventoryDate, String inventoryDateFieldName){
        List<Column> columns = new ArrayList<>();
        columns.add(lit(inventoryDate).as(inventoryDateFieldName));
        columns.addAll(getColumnsFromAnnotationAsOriginal(Arrays.asList(Fact.class), CommonColumnSelector.ALIAS_CURRENT, factTable));
        factDimensionRoleList.forEach(role -> columns.addAll(role.getMasterTableFlattenedFields()));
        return columns.toArray(new Column[columns.size()]);
    }

}
