package com.starschema.columnSelector;

import com.starschema.Alias;
import com.starschema.annotations.dimensions.CheckSum;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.dimensions.UpdatedDate;
import com.starschema.annotations.facts.Fact;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.lookup.AbstractLookup;
import com.utils.ReflectUtils;
import org.apache.spark.sql.Column;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class AnnotatedJunkDimensionColumnSelector<T extends IJunkDimension> extends CommonColumnSelector<T> implements JunkDimensionColumnSelector {

    public AnnotatedJunkDimensionColumnSelector(Class<T> targetClass) {
        super(targetClass);
    }

    @Override
    public Column[] getStagingColumns(LocalDate inventoryDate) {
        List<Column> columnList = new ArrayList<>();

        ReflectUtils.getFields(Arrays.asList(TechnicalId.class, Fact.class), targetClass).forEach(
                field -> columnList.add(col(field.getName()))
        );

        columnList.add(getInventoryDateAsColumn(inventoryDate).as(getUpdatedDateName()));

        Optional<Column> checkSumColumn = calculateCheckSumColumn(Fact.class, CheckSum.class);
        checkSumColumn.ifPresent(columnList::add);

        return columnList.toArray(new Column[columnList.size()]);
    }

    @Override
    public Column[] getNewLinesColumns() {
        List<Column> allColumns = new ArrayList<>();
        allColumns.add(lit(Long.MAX_VALUE).as(getTechnicalIdName()));
        allColumns.addAll(replaceAliasColumnListAsColumnList(Alias.STAGE.getLabel(),
                Arrays.asList(Fact.class, UpdatedDate.class, CheckSum.class)
        ));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

    @Override
    public String getCheckSumColumn(String alias) {
        return addAlias(ReflectUtils.getUniqueField(CheckSum.class, targetClass).get().getName(), alias);
    }



    @Override
    public Column[] getLookupTableColumns(Class lookupClass) {

        List<Column> allColumns = new ArrayList<>();
        String lookupFunctionalFieldName = getFunctionalName(lookupClass);
        String lookupTechnicalFieldName = getTechnicalIdName(lookupClass);

        allColumns.add(getUniqueColumnFromAnnotation(TechnicalId.class, lookupTechnicalFieldName));
        allColumns.add(getUniqueColumnFromAnnotation(CheckSum.class, lookupFunctionalFieldName));

        return allColumns.toArray(new Column[allColumns.size()]);
    }

}
