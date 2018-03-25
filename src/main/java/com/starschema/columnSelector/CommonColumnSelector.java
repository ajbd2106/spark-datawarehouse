package com.starschema.columnSelector;

import com.starschema.annotations.common.Table;
import com.starschema.annotations.dimensions.*;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public abstract class CommonColumnSelector<T> {

    protected static final DateTimeFormatter sparkDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    protected Class<T> targetClass;

    public CommonColumnSelector(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    public String getNewTableName() {
        return targetClass.getAnnotation(Table.class).name();
    }

    public String getMasterTableName() {
        return targetClass.getAnnotation(Table.class).masterTable();
    }

    public String getOldTable() {
        String oldTable = targetClass.getAnnotation(Table.class).oldTable();
        if (oldTable.equals(StringUtils.EMPTY)) {
            return String.format("%s_old", getMasterTableName());
        }
        return oldTable;
    }

    public Column inventoryDateBetweenStartAndEndDate(LocalDate inventoryDate, Class targetClass) {
        return col(getStartDateName(targetClass)).leq(to_date(lit(sparkDateFormatter.format(inventoryDate))))
                .and(col(getEndDateName(targetClass)).geq(to_date(lit(sparkDateFormatter.format(inventoryDate)))));
    }

    protected String getStartDateName(Class targetClass) {
        return getStartDateName(StringUtils.EMPTY, targetClass);
    }

    protected String getStartDateName() {
        return getStartDateName(StringUtils.EMPTY, targetClass);
    }

    protected String getStartDateName(String alias, Class targetClass) {
        return addAlias(ReflectUtils.getUniqueField(StartDate.class, targetClass).get().getName(), alias);
    }

    protected String getEndDateName() {
        return getEndDateName(StringUtils.EMPTY, targetClass);
    }

    protected String getEndDateName(Class targetClass) {
        return getEndDateName(StringUtils.EMPTY, targetClass);
    }

    protected String getEndDateName(String alias, Class targetClass) {
        return addAlias(ReflectUtils.getUniqueField(EndDate.class, targetClass).get().getName(), alias);
    }

    protected List<Class<? extends Annotation>> getAllDimensionAnnotation() {
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

    private Column calculateCheckSumColumn(List<Column> columnList, String alias) {
        return md5(concat_ws(";", columnList.toArray(new Column[columnList.size()]))).as(alias);
    }

    public String getColumnNameWithAlias(String alias, String columnName) {
        return String.format("%s.%s", alias, columnName);
    }

    protected Column getColumnAsOriginal(String alias, String columnName) {
        return col(addAlias(columnName, alias)).as(columnName);
    }

    protected String addAlias(String fieldName, String alias) {
        if (StringUtils.isNotEmpty(alias)) {
            return getColumnNameWithAlias(alias, fieldName);
        }

        return fieldName;
    }

    public String getFakeFunctionalIdValue() {
        Optional<Field> field = ReflectUtils.getUniqueField(FunctionalId.class, targetClass);
        return field.get().getAnnotation(FunctionalId.class).defaultValue();
    }

    protected Long getFakeTechnicalIdValue(Class targetClass) {
        Optional<Field> field = ReflectUtils.getUniqueField(TechnicalId.class, targetClass);
        return field.get().getAnnotation(TechnicalId.class).defaultValue();
    }

    public String getTechnicalIdName() {
        return getTechnicalIdName(targetClass);
    }

    public String getTechnicalIdName(Class targetClass) {
        return getTechnicalIdName(StringUtils.EMPTY, targetClass);
    }

    private String getTechnicalIdName(String alias, Class targetClass) {
        return addAlias(ReflectUtils.getUniqueField(TechnicalId.class, targetClass).get().getName(), alias);
    }


    public String getFunctionalName() {
        return getFunctionalName(StringUtils.EMPTY);
    }

    public String getFunctionalName(String alias) {
        return getFunctionalName(alias, targetClass);
    }

    public String getFunctionalName(Class targetClass) {
        return getFunctionalName(StringUtils.EMPTY, targetClass);
    }

    public String getFunctionalName(String alias, Class targetClass) {
        return addAlias(ReflectUtils.getUniqueField(FunctionalId.class, targetClass).get().getName(), alias);
    }

    protected String getUpdatedDateName() {
        return getUpdatedDateName(StringUtils.EMPTY);
    }

    private String getUpdatedDateName(String alias) {
        return addAlias(ReflectUtils.getUniqueField(UpdatedDate.class, targetClass).get().getName(), alias);
    }

    public Column[] getLookupTableColumns(Class targetClass) {

        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getColumnsFromAnnotation(Arrays.asList(
                TechnicalId.class,
                FunctionalId.class
        ), targetClass));

        return allColumns.toArray(new Column[allColumns.size()]);
    }


    public Column[] getLookupTableColumns() {
        return getLookupTableColumns(targetClass);
    }


    protected List<Column> getColumnsFromAnnotation(List<Class<? extends Annotation>> annotationList, Class targetClass) {
        List<Field> allFields = ReflectUtils.getFields(annotationList, targetClass);

        List<Column> columnList = allFields.stream().map(
                field -> col(field.getName())
        ).collect(Collectors.toList());

        return columnList;

    }

    protected List<Column> getColumnsFromAnnotation(List<Class<? extends Annotation>> annotationList) {
        return getColumnsFromAnnotation(annotationList, targetClass);

    }

    protected List<Column> getColumnsFromAnnotationAsOriginal(List<Class<? extends Annotation>> annotationList, String alias, Class<?> targetClass) {
        List<Field> allFields = ReflectUtils.getFields(annotationList, targetClass);

        List<Column> columnList = allFields.stream().map(
                field -> col(getColumnNameWithAlias(alias, field.getName())).as(field.getName())
        ).collect(Collectors.toList());

        return columnList;

    }

    protected List<Column> getColumnsFromAnnotationAsOriginal(List<Class<? extends Annotation>> annotationList, String alias) {
        return getColumnsFromAnnotationAsOriginal(annotationList, alias, targetClass);

    }

    protected Column getUniqueColumnFromAnnotation(Class<? extends Annotation> annotation, String alias) {
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


    protected List<Column> replaceAliasColumnListAsColumnList(String prefix, List<Class<? extends Annotation>> annotations) {
        return annotations.stream()
                .flatMap(annotation -> ReflectUtils.getFieldsListWithAnnotation(targetClass, annotation).stream())
                .map(field -> col(addAlias(field.getName(), prefix)).as(field.getName()))
                .collect(Collectors.toList());
    }


    protected Optional<Column> calculateCheckSumColumn(Class<? extends Annotation> baseAnnotation, Class<? extends Annotation> checkSumAnnotation) {
        return calculateCheckSumColumn(baseAnnotation, checkSumAnnotation, StringUtils.EMPTY);
    }

    public Optional<Column> calculateCheckSumColumn(Class<? extends Annotation> baseAnnotation, Class<? extends Annotation> checkSumAnnotation, String alias) {

        Optional<Field> checkSumFieldName = ReflectUtils.getUniqueField(checkSumAnnotation, targetClass);

        return checkSumFieldName.flatMap(
                field -> calculateChecksumColumn(field, baseAnnotation, alias)
        );
    }

    private Optional<Column> calculateChecksumColumn(Field checkSumField, Class<? extends Annotation> baseAnnotation, String alias) {
        List<Column> sortedColumnList = ReflectUtils.getSortedFields(Arrays.asList(baseAnnotation), targetClass).stream().map(
                field -> col(addAlias(field.getName(), alias))

        ).collect(Collectors.toList());

        if (sortedColumnList.size() > 0) {
            return Optional.of(calculateCheckSumColumn(sortedColumnList, addAlias(checkSumField.getName(), alias)));
        }

        return Optional.empty();
    }

    protected Column getInventoryDateAsColumn(LocalDate inventoryDate) {
        return getInventoryDateAsColumn(inventoryDate, null);
    }

    protected Column getInventoryDateAsColumn(LocalDate inventoryDate, Integer substract) {
        Column invDate = to_date(lit(sparkDateFormatter.format(inventoryDate)));

        return Optional.ofNullable(substract)
                .flatMap(subs -> Optional.of(date_sub(invDate, subs)))
                .orElse(invDate);
    }
}
