package com.starschema.columnSelector;

import com.starschema.lookup.AbstractLookup;
import org.apache.spark.sql.Column;

import java.lang.annotation.Annotation;
import java.time.LocalDate;
import java.util.Optional;

public interface JunkDimensionColumnSelector {
    Column[] getStagingColumns(LocalDate inventoryDate);

    Column[] getNewLinesColumns();

    String getCheckSumColumn(String alias);

    Column[] getLookupTableColumns(Class<? extends AbstractLookup> lookupClass);

    String getTechnicalIdName();

    String getFunctionalName(Class<? extends AbstractLookup> lookupClass);

    String getFunctionalName(String label, Class<? extends AbstractLookup> lookupClass);

    Optional<Column> calculateCheckSumColumn(Class<? extends Annotation> baseAnnotation, Class<? extends Annotation> checkSumAnnotation, String columnAlias);
}
