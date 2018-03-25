package com.starschema.columnSelector;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;

import java.time.LocalDate;
import java.util.Optional;

public interface DimensionColumnSelector {

    Column[] getStageColumns();

    Column[] getInactiveLinesColumns();

    Column[] getFakeRowColumns();

    Column[] getAllColumnsAsOriginal(String alias);

    Column[] getNewSCD2Columns(LocalDate inventoryDate, String currentAlias, String stageAlias);

    Column[] getLookupTableColumns();

    Column[] getSCD2ColumnsToDeactivate(LocalDate inventoryDate, String currentAlias);

    Column[] getSCD1Columns(String currentAlias, String stageAlias);

    Column[] getNewLinesColumns(LocalDate inventoryDate, String stageAlias);

    Optional<Column> isSCD1AndSCD2Equal(String currentAlias, String stageAlias);

    Optional<Column> isSCD1(String currentAlias, String stageAlias);

    Optional<Column> isSCD2(String currentAlias, String stageAlias);

    String getTechnicalIdName();

    String getFunctionalName();

    String getCurrentFlagName();

    Row getFakeRow(LocalDate inventoryDate, String[] columns);

    Object getFakeFunctionalIdValue();

    String getOldTable();

    String getMasterTableName();

    String getNewTableName();

    String getColumnNameWithAlias(String aliasStage, String functionalName);
}
