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
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class AnnotatedDimensionColumnSelector<T extends Dimension> extends CommonColumnSelector<T> implements DimensionColumnSelector {

    private final String DEFAULT_END_DATE = "2200-01-01";
    private final LocalDate CURRENT_DATE = LocalDate.now();

    public AnnotatedDimensionColumnSelector(Class<T> targetClass) {
        super(targetClass);
    }

    @Override
    public Column[] getStageColumns() {
        List<Column> columnList = new ArrayList<>();

        Optional<Field> functionalIdField = ReflectUtils.getUniqueField(FunctionalId.class, targetClass);
        columnList.add(col(functionalIdField.get().getName()));

        ReflectUtils.getFields(Arrays.asList(SCD1.class, SCD2.class), targetClass)
                .stream()
                .map(field -> col(field.getName()))
                .forEach(columnList::add);

        calculateCheckSumColumn(SCD1.class, SCD1CheckSum.class).ifPresent(columnList::add);
        calculateCheckSumColumn(SCD2.class, SCD2CheckSum.class).ifPresent(columnList::add);

        return columnList.toArray(new Column[columnList.size()]);
    }

    @Override
    public Column[] getInactiveLinesColumns() {

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
                ));

        return columns.toArray(new Column[columns.size()]);

    }

    @Override
    public Column[] getFakeRowColumns() {

        List<Column> columns = getColumnsFromAnnotation(
                Arrays.asList(TechnicalId.class,
                        FunctionalId.class,
                        SCD1.class,
                        SCD2.class,
                        StartDate.class,
                        EndDate.class,
                        UpdatedDate.class,
                        Current.class
                ));

        Optional<Column> scd1CheckSumColumn = calculateCheckSumColumn(SCD1.class, SCD1CheckSum.class);
        Optional<Column> scd2CheckSumColumn = calculateCheckSumColumn(SCD2.class, SCD2CheckSum.class);

        scd1CheckSumColumn.ifPresent(columns::add);
        scd2CheckSumColumn.ifPresent(columns::add);

        return columns.toArray(new Column[columns.size()]);

    }

    @Override
    public Column[] getAllColumnsAsOriginal(String alias) {

        List<Column> columns = getColumnsFromAnnotationAsOriginal(
                getAllDimensionAnnotation(), alias);

        return columns.toArray(new Column[columns.size()]);
    }

    @Override
    public String getCurrentFlagName() {
        return getCurrentFlagName(StringUtils.EMPTY);
    }

    @Override
    public Column[] getNewSCD2Columns(LocalDate inventoryDate, String currentAlias, String stageAlias) {

        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName()));
        allColumns.add(getColumnAsOriginal(currentAlias, getFunctionalName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(SCD1.class, SCD2.class)));
        allColumns.add(getInventoryDateAsColumn(inventoryDate).as(getStartDateName()));
        allColumns.add(to_date(lit(DEFAULT_END_DATE)).as(getEndDateName()));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName()));
        allColumns.add(lit("Y").as(getCurrentFlagName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(SCD1CheckSum.class, SCD2CheckSum.class)));

        return allColumns.toArray(new Column[allColumns.size()]);
    }


    @Override
    public Column[] getSCD2ColumnsToDeactivate(LocalDate inventoryDate, String currentAlias) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias, Arrays.asList(
                TechnicalId.class,
                FunctionalId.class,
                SCD1.class,
                SCD2.class,
                StartDate.class
        )));
        allColumns.add(getInventoryDateAsColumn(inventoryDate, 1).as(getEndDateName()));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName()));
        allColumns.add(lit("N").as(getCurrentFlagName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias, Arrays.asList(
                SCD1CheckSum.class,
                SCD2CheckSum.class
        )));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    @Override
    public Column[] getSCD1Columns(String currentAlias, String stageAlias) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias,
                Arrays.asList(
                        TechnicalId.class,
                        FunctionalId.class
                )));
        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(SCD1.class)));
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias, Arrays.asList(
                SCD2.class,
                StartDate.class,
                EndDate.class
        )));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias, Arrays.asList(Current.class)));
        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(SCD1CheckSum.class)));
        allColumns.addAll(replaceAliasColumnListAsColumnList(currentAlias, Arrays.asList(SCD2CheckSum.class)));
        return allColumns.toArray(new Column[allColumns.size()]);
    }

    @Override
    public Column[] getNewLinesColumns(LocalDate inventoryDate, String stageAlias) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(FunctionalId.class,
                SCD1.class,
                SCD2.class
        )));
        allColumns.add(getInventoryDateAsColumn(inventoryDate).as(getStartDateName()));
        allColumns.add(to_date(lit(DEFAULT_END_DATE)).as(getEndDateName()));
        allColumns.add(getInventoryDateAsColumn(CURRENT_DATE).as(getUpdatedDateName()));
        allColumns.add(lit("Y").as(getCurrentFlagName()));

        allColumns.addAll(replaceAliasColumnListAsColumnList(stageAlias, Arrays.asList(SCD1CheckSum.class,
                SCD2CheckSum.class)));


        return allColumns.toArray(new Column[allColumns.size()]);
    }

    @Override
    public Optional<Column> isSCD1AndSCD2Equal(String currentAlias, String stageAlias) {
        Optional<String> scd1CurrentCheckSumName = getSCD1ChecksumName(currentAlias);
        Optional<String> scd1StageCheckSumName = getSCD1ChecksumName(stageAlias);
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(currentAlias);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(stageAlias);

        Column iScd1Equal = scd1CurrentCheckSumName
                .flatMap(currentChecksum -> Optional.of(isScdChecksumEqual(currentChecksum, scd1StageCheckSumName.get())))
                .orElse(null);

        Column isScd2Equal = scd2CurrentCheckSumName
                .flatMap(currentChecksum -> Optional.of(isScdChecksumEqual(currentChecksum, scd2StageCheckSumName.get())))
                .orElse(null);

        if (Objects.nonNull(iScd1Equal) && Objects.nonNull(isScd2Equal)) {
            return Optional.of(iScd1Equal.and(isScd2Equal));
        }

        if (Objects.nonNull(iScd1Equal)) {
            return Optional.of(iScd1Equal);
        }

        return Optional.ofNullable(isScd2Equal);
    }

    @Override
    public Optional<Column> isSCD1(String currentAlias, String stageAlias) {

        Optional<String> scd1CurrentCheckSumName = getSCD1ChecksumName(currentAlias);
        Optional<String> scd1StageCheckSumName = getSCD1ChecksumName(stageAlias);
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(currentAlias);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(stageAlias);

        Column isScd1Different = scd1CurrentCheckSumName
                .flatMap(currentChecksum -> Optional.of(isScdChecksumDifferent(currentChecksum, scd1StageCheckSumName.get())))
                .orElse(null);

        Column isScd2Equal = scd2CurrentCheckSumName
                .flatMap(currentChecksum -> Optional.of(isScdChecksumEqual(currentChecksum, scd2StageCheckSumName.get())))
                .orElse(null);

        //if scd1 and scd are present
        if (Objects.nonNull(isScd1Different) && Objects.nonNull(isScd2Equal)) {
            return Optional.of(isScd1Different.and(isScd2Equal));
        }

        return Optional.ofNullable(isScd1Different);
    }


    private Column isScdChecksumDifferent(String currentChecksum, String stagingChecksum) {
        return col(currentChecksum).notEqual(col(stagingChecksum));
    }

    private Column isScdChecksumEqual(String currentChecksum, String stagingChecksum) {
        return col(currentChecksum).equalTo(col(stagingChecksum));
    }

    @Override
    public Optional<Column> isSCD2(String currentAlias, String stageAlias) {
        Optional<String> scd2CurrentCheckSumName = getSCD2ChecksumName(currentAlias);
        Optional<String> scd2StageCheckSumName = getSCD2ChecksumName(stageAlias);

        return scd2CurrentCheckSumName
                .flatMap(scdCurrentChecksum -> Optional.of(isScdChecksumDifferent(scdCurrentChecksum, scd2StageCheckSumName.get())));
    }


    @Override
    public Row getFakeRow(LocalDate inventoryDate, String[] columns) {
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

    private String getCurrentFlagName(String alias) {
        return addAlias(ReflectUtils.getUniqueField(Current.class, targetClass).get().getName(), alias);
    }


    private Optional<String> getSCD1ChecksumName(String alias) {
        Optional<Field> scd1Field = ReflectUtils.getUniqueField(SCD1CheckSum.class, targetClass);

        return scd1Field
                .flatMap(scd1 -> Optional.of(addAlias(scd1.getName(), alias)));
    }

    private Optional<String> getSCD2ChecksumName(String alias) {
        Optional<Field> scd2Field = ReflectUtils.getUniqueField(SCD2CheckSum.class, targetClass);
        return scd2Field
                .flatMap(scd2 -> Optional.of(addAlias(scd2.getName(), alias)));
    }


    private Object getDefaultColumnValue(Field field, String defaultValue) {
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
