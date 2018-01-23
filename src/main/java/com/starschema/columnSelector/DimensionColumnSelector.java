package com.starschema.columnSelector;

import com.starschema.annotations.dimensions.*;
import com.starschema.dimension.Dimension;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class DimensionColumnSelector extends CommonColumnSelector {

    private static final String DEFAULT_END_DATE = "2200-01-01";
    private static final LocalDate CURRENT_DATE = LocalDate.now();

    public static Column[] getStageColumns(Class<? extends Dimension> targetClass) {
        List<Column> columnList = new ArrayList<>();

        Optional<Field> functionalIdField = ReflectUtils.getUniqueField(FunctionalId.class, targetClass);
        columnList.add(col(functionalIdField.get().getName()));

        ReflectUtils.getFields(Arrays.asList(SCD1.class, SCD2.class), targetClass).forEach(
                field -> {
                    Column column = col(field.getName());
                    columnList.add(column);
                }
        );

        Optional<Column> scd1CheckSumColumn = getCheckSumColumn(SCD1.class, SCD1CheckSum.class, targetClass);
        Optional<Column> scd2CheckSumColumn = getCheckSumColumn(SCD2.class, SCD2CheckSum.class, targetClass);

        if (scd1CheckSumColumn.isPresent()) {
            columnList.add(scd1CheckSumColumn.get());
        }

        if (scd2CheckSumColumn.isPresent()) {
            columnList.add(scd2CheckSumColumn.get());
        }

        return columnList.toArray(new Column[columnList.size()]);
    }


    public static Column[] getInactiveLinesColumns(Class<? extends Dimension> targetClass) {

        List<Column> columns = getColumnsFromAnnotation(
                Arrays.asList(TechnicalId.class,
                        FunctionalId.class,
                        SCD1.class,
                        SCD2.class,
                        StartDate.class,
                        EndDate.class,
                        UpdatedDate.class,
                        Current.class,
                        SCD1CheckSum.class,
                        SCD2CheckSum.class
                ), targetClass);

        return columns.toArray(new Column[columns.size()]);

    }

    public static Column[] getFakeRowColumns(Class<? extends Dimension> targetClass) {

        List<Column> columns = getColumnsFromAnnotation(
                Arrays.asList(TechnicalId.class,
                        FunctionalId.class,
                        SCD1.class,
                        SCD2.class,
                        StartDate.class,
                        EndDate.class,
                        UpdatedDate.class,
                        Current.class
                ), targetClass);

        Optional<Column> scd1CheckSumColumn = getCheckSumColumn(SCD1.class, SCD1CheckSum.class, targetClass);
        Optional<Column> scd2CheckSumColumn = getCheckSumColumn(SCD2.class, SCD2CheckSum.class, targetClass);

        if (scd1CheckSumColumn.isPresent()) {
            columns.add(scd1CheckSumColumn.get());
        }

        if (scd2CheckSumColumn.isPresent()) {
            columns.add(scd2CheckSumColumn.get());
        }

        return columns.toArray(new Column[columns.size()]);

    }

    public static Column[] getAllColumnsAsOriginal(String alias, Class<? extends Dimension> targetClass) {

        List<Column> columns = getColumnsFromAnnotationAsOriginal(
                getAllDimensionAnnotation(), alias, targetClass);

        return columns.toArray(new Column[columns.size()]);
    }

    public static String getCurrentFlagName(Class<? extends Dimension> tableClass) {
        return getCurrentFlagName(StringUtils.EMPTY, tableClass);
    }

    public static String getCurrentFlagName(String alias, Class<? extends Dimension> tableClass) {
        return addAlias(ReflectUtils.getUniqueField(Current.class, tableClass).get().getName(), alias);
    }

    public static String getStartDateName(Class<? extends Dimension> tableClass) {
        return getStartDateName(StringUtils.EMPTY, tableClass);
    }

    public static String getStartDateName(String alias, Class<? extends Dimension> tableClass) {
        return addAlias(ReflectUtils.getUniqueField(StartDate.class, tableClass).get().getName(), alias);
    }

    public static String getEndDateName(Class<? extends Dimension> tableClass) {
        return getEndDateName(StringUtils.EMPTY, tableClass);
    }

    public static String getEndDateName(String alias, Class<? extends Dimension> tableClass) {
        return addAlias(ReflectUtils.getUniqueField(EndDate.class, tableClass).get().getName(), alias);
    }


    public static Optional<String> getSCD1ChecksumName(String alias, Class<? extends Dimension> targetClass) {
        Optional<Field> scd1Field = ReflectUtils.getUniqueField(SCD1CheckSum.class, targetClass);

        if (scd1Field.isPresent()) {
            return Optional.of(addAlias(scd1Field.get().getName(), alias));
        }
        return Optional.empty();
    }

    public static Optional<String> getSCD2ChecksumName(String alias, Class<? extends Dimension> targetClass) {
        Optional<Field> scd2Field = ReflectUtils.getUniqueField(SCD2CheckSum.class, targetClass);

        if (scd2Field.isPresent()) {
            return Optional.of(addAlias(scd2Field.get().getName(), alias));
        }
        return Optional.empty();
    }


    private static List<Column> getStageColumnListAsOriginalColumnList(List<Class<? extends Annotation>> annotations, Class<? extends Dimension> targetClass) {
        return replaceAliasColumnListAsColumnList(ALIAS_STAGE, annotations, targetClass);
    }

    private static List<Column> getCurrentColumnListAsOriginalColumnList(List<Class<? extends Annotation>> annotations, Class<? extends Dimension> targetClass) {
        return replaceAliasColumnListAsColumnList(ALIAS_CURRENT, annotations, targetClass);
    }


    public static Column[] getNewSCD2Columns(LocalDate inventoryDate, Class<? extends Dimension> targetClass) {

        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName(targetClass)));
        allColumns.add(getColumnAsOriginal(ALIAS_CURRENT, getFunctionalName(targetClass)));
        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(SCD1.class, SCD2.class), targetClass));
        allColumns.add(getInventoryDateAsColumn(inventoryDate).as(getStartDateName(targetClass)));
        allColumns.add(to_date(lit(DEFAULT_END_DATE)).as(getEndDateName(targetClass)));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName(targetClass)));
        allColumns.add(lit("Y").as(getCurrentFlagName(targetClass)));
        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(SCD1CheckSum.class, SCD2CheckSum.class), targetClass));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static Column[] getLookupTableColumns(Class<? extends Dimension> tableClass) {

        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getColumnsFromAnnotation(Arrays.asList(
                TechnicalId.class,
                FunctionalId.class
        ), tableClass));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static Column[] getSCD2ColumnsToDeactivate(LocalDate inventoryDate, Class<? extends Dimension> targetClass) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(Arrays.asList(
                TechnicalId.class,
                FunctionalId.class,
                SCD1.class,
                SCD2.class,
                StartDate.class
        ), targetClass));
        allColumns.add(getInventoryDateAsColumn(inventoryDate, 1).as(getEndDateName(targetClass)));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName(targetClass)));
        allColumns.add(lit("N").as(getCurrentFlagName(targetClass)));
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(Arrays.asList(
                SCD1CheckSum.class,
                SCD2CheckSum.class
        ), targetClass));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static Column[] getSCD1Columns(LocalDate inventoryDate, Class<? extends Dimension> targetClass) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(
                Arrays.asList(
                        TechnicalId.class,
                        FunctionalId.class
                ), targetClass));
        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(SCD1.class), targetClass));
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(Arrays.asList(
                SCD2.class,
                StartDate.class,
                EndDate.class
        ), targetClass));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName(targetClass)));
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(Arrays.asList(Current.class), targetClass));
        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(SCD1CheckSum.class), targetClass));
        allColumns.addAll(getCurrentColumnListAsOriginalColumnList(Arrays.asList(SCD2CheckSum.class), targetClass));
        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static Column[] getNewLinesColumns(LocalDate inventoryDate, Class<? extends Dimension> targetClass) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName(targetClass)));
        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(FunctionalId.class,
                SCD1.class,
                SCD2.class
        ), targetClass));
        allColumns.add(getInventoryDateAsColumn(inventoryDate).as(getStartDateName(targetClass)));
        allColumns.add(to_date(lit(DEFAULT_END_DATE)).as(getEndDateName(targetClass)));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName(targetClass)));
        allColumns.add(lit("Y").as(getCurrentFlagName(targetClass)));

        allColumns.addAll(getStageColumnListAsOriginalColumnList(Arrays.asList(SCD1CheckSum.class,
                SCD2CheckSum.class), targetClass));


        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static Optional<Column> isSCD1AndSCD2Equal(Class<? extends Dimension> targetClass) {
        Optional<String> scd1CurrentCheckSumName = getSCD1ChecksumName(ALIAS_CURRENT, targetClass);
        Optional<String> scd1StageCheckSumName = getSCD1ChecksumName(ALIAS_STAGE, targetClass);
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(ALIAS_CURRENT, targetClass);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(ALIAS_STAGE, targetClass);

        Column scd1Column = null;
        Column scd2Column = null;

        if (scd1CurrentCheckSumName.isPresent()) {
            scd1Column = col(scd1CurrentCheckSumName.get()).equalTo(col(scd1StageCheckSumName.get()));
        }

        if (scd2CurrentCheckSumName.isPresent()) {
            scd2Column = col(scd2CurrentCheckSumName.get()).equalTo(col(scd2StageCheckSumName.get()));
        }

        if (scd1CurrentCheckSumName.isPresent() && scd2CurrentCheckSumName.isPresent()) {
            return Optional.of(scd1Column.and(scd2Column));
        }

        if (scd1CurrentCheckSumName.isPresent()) {
            return Optional.of(scd1Column);
        }

        if (scd2CurrentCheckSumName.isPresent()) {
            return Optional.of(scd2Column);
        }

        return Optional.empty();
    }


    public static Optional<Column> isSCD1(Class<? extends Dimension> targetClass) {

        Optional<String> scd1CurrentCheckSumName = getSCD1ChecksumName(ALIAS_CURRENT, targetClass);
        Optional<String> scd1StageCheckSumName = getSCD1ChecksumName(ALIAS_STAGE, targetClass);
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(ALIAS_CURRENT, targetClass);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(ALIAS_STAGE, targetClass);

        Column scd1Column = null;
        Column scd2Column = null;

        if (scd1CurrentCheckSumName.isPresent()) {
            scd1Column = col(scd1CurrentCheckSumName.get()).notEqual(col(scd1StageCheckSumName.get()));
        }

        if (scd2CurrentCheckSumName.isPresent()) {
            scd2Column = col(scd2CurrentCheckSumName.get()).equalTo(col(scd2StageCheckSumName.get()));
        }

        if (scd1CurrentCheckSumName.isPresent() && scd2CurrentCheckSumName.isPresent()) {
            return Optional.of(scd1Column.and(scd2Column));
        }

        if (scd1CurrentCheckSumName.isPresent()) {
            return Optional.of(scd1Column);
        }

        return Optional.empty();
    }


    public static Optional<Column> isSCD2(Class<? extends Dimension> targetClass) {
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(ALIAS_CURRENT, targetClass);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(ALIAS_STAGE, targetClass);

        if (scd2CurrentCheckSumName.isPresent()) {
            return Optional.of(col(scd2CurrentCheckSumName.get()).notEqual(col(scd2StageCheckSumName.get())));
        }

        return Optional.empty();
    }

    public static Column inventoryDateBetweenStartAndEndDate(LocalDate inventoryDate, Class<? extends Dimension> tableClass) {
        return col(getStartDateName(tableClass)).leq(to_date(lit(sparkDateFormatter.format(inventoryDate))))
                .and(col(getEndDateName(tableClass)).geq(to_date(lit(sparkDateFormatter.format(inventoryDate)))));
    }


    public static Row getFakeRow(LocalDate inventoryDate, String[] columns, Class<? extends Dimension> targetClass) {
        Object[] columnValues = new Object[columns.length];


        for (int i = columns.length - 1; i >= 0; i--) {
            String column = columns[i];
            Field field = FieldUtils.getField(targetClass, column, true);
            Annotation[] annotation = field.getDeclaredAnnotations();

            for (Annotation currentAnnotation : annotation) {

                if (currentAnnotation instanceof TechnicalId) {
                    columnValues[i] = ((TechnicalId) currentAnnotation).defaultValue();
                    break;
                }

                if (currentAnnotation instanceof FunctionalId) {
                    columnValues[i] = ((FunctionalId) currentAnnotation).defaultValue();
                    break;
                }

                if (currentAnnotation instanceof SCD1) {
                    columnValues[i] = getDefaultColumnValue(field, ((SCD1) currentAnnotation).defaultValue());
                    break;
                }

                if (currentAnnotation instanceof SCD2) {
                    columnValues[i] = getDefaultColumnValue(field, ((SCD2) currentAnnotation).defaultValue());
                    break;
                }

                if (currentAnnotation instanceof StartDate) {
                    columnValues[i] = Date.valueOf(inventoryDate);
                    break;
                }

                if (currentAnnotation instanceof EndDate) {
                    columnValues[i] = Date.valueOf(LocalDate.from(sparkDateFormatter.parse(((EndDate) currentAnnotation).defaultValue())));
                    break;
                }

                if (currentAnnotation instanceof UpdatedDate) {
                    columnValues[i] = Date.valueOf(CURRENT_DATE);
                    break;
                }

                if (currentAnnotation instanceof Current) {
                    columnValues[i] = ((Current) currentAnnotation).defaultValue();
                    break;
                }

                if (currentAnnotation instanceof SCD1CheckSum) {
                    columnValues[i] = "0";
                    break;
                }

                if (currentAnnotation instanceof SCD2CheckSum) {
                    columnValues[i] = "0";
                    break;
                }
            }
        }

        return RowFactory.create(columnValues);

    }

    private static Object getDefaultColumnValue(Field field, String defaultValue) {
        Class<?> fieldType = field.getType();

        if (defaultValue == null) {
            return defaultValue;
        }

        if (String.class.isAssignableFrom(fieldType)) {
            return defaultValue;
        }

        if (Long.class.isAssignableFrom(fieldType)) {
            try {
                return Long.parseLong(defaultValue);
            } catch (NumberFormatException e) {
                throw new RuntimeException(String.format("defaultValue %s cannot be parsed as long for field %s", defaultValue, field.getName()), e);
            }
        }

        if (Integer.class.isAssignableFrom(fieldType)) {
            try {
                return Integer.parseInt(defaultValue);
            } catch (NumberFormatException e) {
                throw new RuntimeException(String.format("defaultValue %s cannot be parsed as integer for field %s", defaultValue, field.getName()), e);
            }
        }

        if (BigDecimal.class.isAssignableFrom(fieldType)) {
            try {
                return new BigDecimal(defaultValue);
            } catch (NumberFormatException e) {
                throw new RuntimeException(String.format("defaultValue %s cannot be parsed as big decimal for field %s", defaultValue, field.getName()), e);
            }
        }

        if (Date.class.isAssignableFrom(fieldType)) {
            try {
                return new Date(dateFormatter.parse(defaultValue).getTime());
            } catch (ParseException e) {
                throw new RuntimeException(String.format("defaultValue %s is in the wrong date format for field %s", defaultValue, field.getName()), e);
            }
        }

        throw new RuntimeException(String.format("type %s is not handled by default column value for field %s", fieldType, field.getName()));
    }


}
