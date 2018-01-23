package com.starschema.columnSelector;

import com.starschema.annotations.dimensions.*;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public abstract class CommonColumnSelector {

    public static final String ALIAS_STAGE = "stage";
    public static final String ALIAS_CURRENT = "current";

    protected static final DateTimeFormatter sparkDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public static List<Class<? extends Annotation>> getAllDimensionAnnotation() {
        return Arrays.asList(
                TechnicalId.class,
                FunctionalId.class,
                SCD1.class,
                SCD2.class,
                StartDate.class,
                EndDate.class,
                UpdatedDate.class,
                Current.class,
                SCD1CheckSum.class,
                SCD2CheckSum.class);
    }

    public static Column calculateCheckSumColumn(List<Column> columnList, String alias) {
        return md5(concat_ws(";", columnList.toArray(new Column[columnList.size()]))).as(alias);
    }

    protected static List<Column> getColumnsFromAnnotation(List<Class<? extends Annotation>> annotationList, Class<?> targetClass) {
        List<Field> allFields = ReflectUtils.getFields(annotationList, targetClass);

        List<Column> columnList = allFields.stream().map(
                field -> col(field.getName())
        ).collect(Collectors.toList());

        return columnList;

    }

    public static String getColumnNameWithAlias(String alias, String columnName) {
        return String.format("%s.%s", alias, columnName);
    }


    public static Column getColumnAsOriginal(String alias, String columnName) {
        return col(addAlias(columnName, alias)).as(columnName);
    }

    protected static List<Column> getColumnsFromAnnotationAsOriginal(List<Class<? extends Annotation>> annotationList, String alias, Class<?> targetClass) {
        List<Field> allFields = ReflectUtils.getFields(annotationList, targetClass);

        List<Column> columnList = allFields.stream().map(
                field -> col(getColumnNameWithAlias(alias, field.getName())).as(field.getName())
        ).collect(Collectors.toList());

        return columnList;

    }

    protected static Column getUniqueColumnFromAnnotation(Class<? extends Annotation> annotation, String alias, Class<?> targetClass) {
        Optional<Field> uniqueField = ReflectUtils.getUniqueField(annotation, targetClass);

        if (!uniqueField.isPresent()) {
            throw new RuntimeException(String.format("Field with annotation %s has not been found on class %s", annotation.getName(), targetClass.getCanonicalName()));
        }

        Column column = col(uniqueField.get().getName());

        if (StringUtils.isNotEmpty(alias)) {
            return column.as(alias);
        }

        return column;


    }

    public static String addAlias(String fieldName, String alias) {
        if (StringUtils.isNotEmpty(alias)) {
            return getColumnNameWithAlias(alias, fieldName);
        }

        return fieldName;
    }

    protected static List<Column> replaceAliasColumnListAsColumnList(String prefix, List<Class<? extends Annotation>> annotations, Class<?> targetClass) {
        return annotations.stream()
                .flatMap(annotation -> ReflectUtils.getFieldsListWithAnnotation(targetClass, annotation).stream())
                .map(field -> col(addAlias(field.getName(), prefix)).as(field.getName()))
                .collect(Collectors.toList());
    }

    public static String getFakeFunctionalIdValue(Class className) {
        Optional<Field> field = ReflectUtils.getUniqueField(FunctionalId.class, className);
        return field.get().getAnnotation(FunctionalId.class).defaultValue();
    }

    public static Long getFakeTechnicalIdValue(Class className) {
        Optional<Field> field = ReflectUtils.getUniqueField(TechnicalId.class, className);
        return field.get().getAnnotation(TechnicalId.class).defaultValue();
    }

    public static String getTechnicalIdName(Class<?> className) {
        return getTechnicalIdName(StringUtils.EMPTY, className);
    }

    public static String getTechnicalIdName(String alias, Class<?> className) {
        return addAlias(ReflectUtils.getUniqueField(TechnicalId.class, className).get().getName(), alias);
    }


    public static String getFunctionalName(Class<?> className) {
        return getFunctionalName(StringUtils.EMPTY, className);
    }

    public static String getFunctionalName(String alias, Class<?> className) {
        return addAlias(ReflectUtils.getUniqueField(FunctionalId.class, className).get().getName(), alias);
    }

    public static String getUpdatedDateName(Class<?> className) {
        return getUpdatedDateName(StringUtils.EMPTY, className);
    }

    public static String getUpdatedDateName(String alias, Class<?> className) {
        return addAlias(ReflectUtils.getUniqueField(UpdatedDate.class, className).get().getName(), alias);
    }

    protected static Optional<Column> getCheckSumColumn(Class<? extends Annotation> baseAnnotation, Class<? extends Annotation> checkSumAnnotation, Class<?> targetClass) {
        return getCheckSumColumn(baseAnnotation, checkSumAnnotation, targetClass, StringUtils.EMPTY);
    }


    public static Optional<Column> getCheckSumColumn(Class<? extends Annotation> baseAnnotation, Class<? extends Annotation> checkSumAnnotation, Class<?> tableClass, String alias) {

        Optional<Field> checkSumFieldName = ReflectUtils.getUniqueField(checkSumAnnotation, tableClass);

        if (checkSumFieldName.isPresent()) {

            List<Column> columnList = ReflectUtils.getSortedFields(Arrays.asList(baseAnnotation), tableClass).stream().map(
                    field -> col(addAlias(field.getName(), alias))

            ).collect(Collectors.toList());

            if (columnList.size() > 0) {
                return Optional.of(calculateCheckSumColumn(columnList, addAlias(checkSumFieldName.get().getName(), alias)));
            }
        }

        return Optional.empty();

    }


    protected static Column getInventoryDateAsColumn(LocalDate inventoryDate){
        return getInventoryDateAsColumn(inventoryDate, null);
    }

    protected static Column getInventoryDateAsColumn(LocalDate inventoryDate, Integer substract){

        Column invDate = to_date(lit(sparkDateFormatter.format(inventoryDate)));
        if(substract == null){
            return invDate;
        }
        return date_sub(invDate, substract);

    }
}
