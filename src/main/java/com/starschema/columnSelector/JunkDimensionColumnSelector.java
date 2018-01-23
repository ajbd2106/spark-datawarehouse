package com.starschema.columnSelector;

import com.starschema.annotations.dimensions.CheckSum;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.dimensions.UpdatedDate;
import com.starschema.annotations.facts.Fact;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.lookup.AbstractLookup;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.Window;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class JunkDimensionColumnSelector extends CommonColumnSelector {

    public static Column[] getStagingColumns(Class<? extends IJunkDimension> targetClass, LocalDate inventoryDate) {
        List<Column> columnList = new ArrayList<>();

        ReflectUtils.getFields(Arrays.asList(TechnicalId.class, Fact.class), targetClass).forEach(
                field -> columnList.add(col(field.getName()))
        );

        columnList.add(getInventoryDateAsColumn(inventoryDate).as(getUpdatedDateName(targetClass)));

        Optional<Column> checkSumColumn = getCheckSumColumn(Fact.class, CheckSum.class, targetClass);
        if (checkSumColumn.isPresent()) {
            columnList.add(checkSumColumn.get());
        }

        return columnList.toArray(new Column[columnList.size()]);
    }


    public static  Column[] getNewLinesColumns(Class<? extends IJunkDimension> targetClass) {
        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName(targetClass)));
        allColumns.addAll(replaceAliasColumnListAsColumnList(ALIAS_STAGE,
                Arrays.asList(Fact.class, UpdatedDate.class, CheckSum.class), targetClass
        ));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    public static  String getCheckSumColumn(String alias, Class<? extends IJunkDimension> targetClass) {
        return addAlias(ReflectUtils.getUniqueField(CheckSum.class, targetClass).get().getName(), alias);
    }

    public static  Column[] getLookupTableColumns(Class<? extends AbstractLookup> lookupClass, Class<? extends IJunkDimension> targetClass) {

        List<Column> allColumns = new ArrayList<>();
        String lookupFunctionalFieldName = getFunctionalName(lookupClass);
        String lookupTechnicalFieldName = getTechnicalIdName(lookupClass);

        allColumns.add(getUniqueColumnFromAnnotation(TechnicalId.class, lookupTechnicalFieldName, targetClass));
        allColumns.add(getUniqueColumnFromAnnotation(CheckSum.class, lookupFunctionalFieldName, targetClass));

        return allColumns.toArray(new Column[allColumns.size()]);
    }


    public static  Column[] getAllJunkDimensionColumnsAsOriginal(String alias, Class<? extends IJunkDimension> targetClass) {
        List<Column> allColumns = getColumnsFromAnnotationAsOriginal(
                Arrays.asList(TechnicalId.class, Fact.class, UpdatedDate.class, CheckSum.class), alias, targetClass);

        return allColumns.toArray(new Column[allColumns.size()]);
    }
}
