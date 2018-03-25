package com.starschema.dimension;

import com.sink.ISink;
import com.source.ISource;
import com.spark.SparkFunctionUtils;
import com.starschema.Alias;
import com.starschema.Processor;
import com.starschema.annotations.dimensions.*;
import com.starschema.columnSelector.DimensionColumnSelector;
import com.utils.ReflectUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

@Slf4j
public class DimensionProcessor<T extends Dimension> implements Processor<T> {

    private final transient SparkSession sparkSession;
    private final Class<T> dimensionClass;
    private final ISource<T> masterDimensionSource;
    private final ISource stagingDimensionSource;
    private final ISink dimensionSink;
    private final ISink lookupSink;
    private final LocalDate inventoryDate;
    private final DimensionColumnSelector dimensionColumnSelector;

    public DimensionProcessor(SparkSession sparkSession, Class<T> dimensionClass, ISink dimensionSink, ISink lookupSink, ISource<T> masterDimensionSource, ISource stagingDimensionSource, LocalDate inventoryDate,
                              DimensionColumnSelector dimensionColumnSelector) {
        validateProcessorClass(dimensionClass);

        this.dimensionClass = dimensionClass;
        this.dimensionSink = dimensionSink;
        this.lookupSink = lookupSink;
        this.sparkSession = sparkSession;
        this.masterDimensionSource = masterDimensionSource;
        this.stagingDimensionSource = stagingDimensionSource;
        this.inventoryDate = inventoryDate;
        this.dimensionColumnSelector = dimensionColumnSelector;
    }

    @Override
    public void process() {

        Column[] allColumnsFromCurrentAlias = dimensionColumnSelector.getAllColumnsAsOriginal(Alias.CURRENT.getLabel());

        Encoder<T> dimensionEncoder = Encoders.bean(dimensionClass);

        Dataset<Row> originalStagingTable = stagingDimensionSource.load(sparkSession);
        Dataset<T> currentTable = masterDimensionSource.load(sparkSession, dimensionEncoder);

        String technicalIdName = dimensionColumnSelector.getTechnicalIdName();
        String currentFlagName = dimensionColumnSelector.getCurrentFlagName();
        String functionalName = dimensionColumnSelector.getFunctionalName();

        Long maxId = SparkFunctionUtils.getMaxRowId(currentTable, technicalIdName);

        // get all active lines
        Dataset<T> activeLines = getActiveLines(currentTable, currentFlagName);

        //here we calculate scd1 and scd2 checksum
        Dataset<Row> stagingTable = originalStagingTable.select(dimensionColumnSelector.getStageColumns());

        //active lines joined with staging table
        Dataset<Row> fullJoinDataset = fullJoinCurrentAndStaging(stagingTable, functionalName, activeLines);

        Dataset<Row> joinedActiveLines = getLinesInStagingAndCurrentTable(fullJoinDataset, functionalName);

        //get all func id in staging table that is not in current table
        Dataset<Row> newLines = getNewLines(fullJoinDataset, functionalName);
        long newLinesCount = newLines.count();
        log.info(String.format("number of new lines %d", newLinesCount));

        //get all scd1
        Optional<Dataset<T>> scd1Lines = getScd1Lines(joinedActiveLines, dimensionEncoder);
        long scd1Count = getCount(scd1Lines);
        log.info(String.format("number of scd1 impacted lines %d", scd1Count));

        //get all scd2
        Optional<Dataset<Row>> scd2Lines = getSCD2Lines(joinedActiveLines);
        long scd2Count = getCount(scd2Lines);
        log.info(String.format("number of scd2 impacted lines %d", scd2Count));

        //In this scenario there are no lines to update and no lines to add comparing to the current table.
        //So we don't need to go further in the process....
        if (newLinesCount == 0l && scd1Count == 0l && scd2Count == 0l) {
            return;
        }

        //updated old scd2 lines
        Optional<Dataset<T>> scd2OldLines = getSCD2LinesToDeactivate(scd2Lines, dimensionEncoder);

        //new lines from scd2
        Optional<Dataset<Row>> scd2NewLines = getSCD2LinesToCreate(scd2Lines);

        //get active lines that are not in the staging table
        Dataset<T> activeLinesNotInStaging = getLinesNotInStagingTable(fullJoinDataset, functionalName, allColumnsFromCurrentAlias, dimensionEncoder);

        //create fake row lines
        //TODO: if a column has been added in a release, fake row should be automatically recalculated. Currently, code only handles first shot scenario.
        Optional<Dataset<T>> fakeRowDataSet = getFakeRowDataSet(activeLines, functionalName, activeLinesNotInStaging, dimensionEncoder);

        if (fakeRowDataSet.isPresent()) {
            activeLinesNotInStaging = activeLinesNotInStaging.union(fakeRowDataSet.get());
        }

        // get all inactive lines
        Dataset<T> inactiveLines = getInactiveLines(currentTable, currentFlagName, dimensionEncoder);

        //get active lines that didn't change at all.
        Optional<Dataset<T>> activeAndUnchanged = getUnchangedLines(allColumnsFromCurrentAlias, joinedActiveLines, dimensionEncoder);

        //just make an union of all old lines to copy
        Dataset<T> updatedLines = getAllLinesWithExistingTechnicalId(inactiveLines, activeLinesNotInStaging, activeAndUnchanged, scd1Lines, scd2OldLines);

        //calculate an incremental technical id for the new lines. Can cause a serious performance problem if there are too many lines, but in the normal process, that should be fine.
        Dataset<T> allNewLines = getAllLinesWithNewTechnicalId(maxId, newLines, scd2NewLines, dimensionEncoder);

        Dataset<T> allRows = allNewLines.union(updatedLines);

        //TODO: We reduce the number of partition, but it should be tuned...
        allRows = allRows.coalesce(2);
        Dataset<Row> idLookupTable = getLookUpTable(currentFlagName, allRows);

        dimensionSink.insert(allRows.select(col("*")));
        lookupSink.insert(idLookupTable);

        dimensionSink.dropTable(dimensionColumnSelector.getOldTable());
        dimensionSink.renameTable(dimensionColumnSelector.getMasterTableName(), dimensionColumnSelector.getOldTable());
        dimensionSink.renameTable(dimensionColumnSelector.getNewTableName(), dimensionColumnSelector.getMasterTableName());
    }

    private void validateProcessorClass(Class<T> dimensionClass) {
        ReflectUtils.checkExistsAndUnique(TechnicalId.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(FunctionalId.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(StartDate.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(EndDate.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(UpdatedDate.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(Current.class, dimensionClass);

        List<Field> scd1Fields = ReflectUtils.getFieldsListWithAnnotation(dimensionClass, SCD1.class);
        if (scd1Fields != null && scd1Fields.size() > 0) {
            ReflectUtils.checkExistsAndUnique(SCD1CheckSum.class, dimensionClass);
        }

        List<Field> scd2Fields = ReflectUtils.getFieldsListWithAnnotation(dimensionClass, SCD2.class);
        if (scd2Fields != null && scd2Fields.size() > 0) {
            ReflectUtils.checkExistsAndUnique(SCD2CheckSum.class, dimensionClass);
        }
    }

    private <U> long getCount(Optional<Dataset<U>> dataset) {
        return dataset
                .flatMap(ds -> Optional.of(ds.count()))
                .orElse(0l);
    }


    private Optional<Dataset<T>> getFakeRowDataSet(Dataset<T> activeLines, String functionalName, Dataset<T> activeLinesNotInStaging, Encoder<T> dimensionEncoder) {
        Dataset<T> fakeRowDataSet = activeLines
                .filter(col(functionalName).equalTo(dimensionColumnSelector.getFakeFunctionalIdValue()));

        if (fakeRowDataSet.count() == 0) {
            Row fakeRow = dimensionColumnSelector.getFakeRow(inventoryDate, activeLinesNotInStaging.columns());
            fakeRowDataSet = sparkSession.createDataFrame(Arrays.asList(fakeRow), activeLinesNotInStaging.schema()).as(dimensionEncoder);
            //here we are calculating scd1 and scd2 checksum
            return Optional.of(fakeRowDataSet
                    .select(dimensionColumnSelector.getFakeRowColumns())
                    .as(dimensionEncoder));
        }

        return Optional.empty();
    }

    private Dataset<Row> getLookUpTable(String currentFlagName, Dataset<T> allRows) {
        return allRows.filter(col(currentFlagName).equalTo("Y"))
                .select(dimensionColumnSelector.getLookupTableColumns());
    }

    private Dataset<T> getAllLinesWithNewTechnicalId(Long maxId, Dataset<Row> newLines, Optional<Dataset<Row>> scd2NewLines, Encoder<T> dimensionEncoder) {
        Dataset<Row> allNewLines = newLines;

        if (scd2NewLines.isPresent()) {
            allNewLines = allNewLines.union(scd2NewLines.get());
        }

        return SparkFunctionUtils.generateTechnicalId(sparkSession, maxId, allNewLines).as(dimensionEncoder);
    }

    private Dataset<T> getAllLinesWithExistingTechnicalId(Dataset<T> inactiveLines, Dataset<T> activeLinesNotInStaging, Optional<Dataset<T>> activeAndUnchanged, Optional<Dataset<T>> scd1Lines, Optional<Dataset<T>> scd2OldLines) {
        Dataset<T> result = inactiveLines
                .union(activeLinesNotInStaging);

        if (activeAndUnchanged.isPresent()) {
            result = result.union(activeAndUnchanged.get());
        }

        if (scd1Lines.isPresent()) {
            result = result.union(scd1Lines.get());
        }

        if (scd2OldLines.isPresent()) {
            result = result.union(scd2OldLines.get());
        }

        return result;
    }

    private Optional<Dataset<Row>> getSCD2LinesToCreate(Optional<Dataset<Row>> scd2Lines) {
        return scd2Lines
                .flatMap(scd2Dataset -> Optional.of(
                        scd2Dataset.select(dimensionColumnSelector.getNewSCD2Columns(inventoryDate, Alias.CURRENT.getLabel(), Alias.STAGE.getLabel()))
                        )
                );
    }

    private Optional<Dataset<T>> getSCD2LinesToDeactivate(Optional<Dataset<Row>> scd2Lines, Encoder<T> dimensionEncoder) {
        return scd2Lines
                .flatMap(scd2Dataset -> Optional.of(
                        scd2Dataset.select(dimensionColumnSelector.getSCD2ColumnsToDeactivate(inventoryDate, Alias.CURRENT.getLabel()))
                                .as(dimensionEncoder)
                        )
                );
    }

    private Optional<Dataset<Row>> getSCD2Lines(Dataset<Row> joinedActiveLines) {
        Optional<Column> columnFilter = dimensionColumnSelector.isSCD2(Alias.CURRENT.getLabel(), Alias.STAGE.getLabel());

        return columnFilter.flatMap(
                filter -> Optional.of(joinedActiveLines.filter(filter))
        );
    }

    private Optional<Dataset<T>> getScd1Lines(Dataset<Row> joinedActiveLines, Encoder<T> dimensionEncoder) {
        Optional<Column> columnFilter = dimensionColumnSelector.isSCD1(Alias.CURRENT.getLabel(), Alias.STAGE.getLabel());

        return columnFilter.flatMap(filter -> Optional.of(
                joinedActiveLines
                        .filter(filter)
                        .select(dimensionColumnSelector.getSCD1Columns(Alias.CURRENT.getLabel(), Alias.STAGE.getLabel()))
                        .as(dimensionEncoder)
                )
        );
    }

    private Optional<Dataset<T>> getUnchangedLines(Column[] allColumnsFromCurrentAlias, Dataset<Row> joinedActiveLines, Encoder<T> dimensionEncoder) {
        Optional<Column> columnFilter = dimensionColumnSelector.isSCD1AndSCD2Equal(Alias.CURRENT.getLabel(), Alias.STAGE.getLabel());

        return columnFilter.flatMap(filter -> Optional.of(
                joinedActiveLines
                        .filter(filter)
                        .select(allColumnsFromCurrentAlias)
                        .as(dimensionEncoder)
                )
        );
    }

    /***
     *
     * @param stagingTable
     * @param functionalName
     * @param activeLines
     * @return return a full outer join between current and staging.
     */
    private Dataset<Row> fullJoinCurrentAndStaging(Dataset<Row> stagingTable, String functionalName, Dataset<T> activeLines) {

        String stagingFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.STAGE.getLabel(), functionalName);
        String currentFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.CURRENT.getLabel(), functionalName);

        return activeLines.alias(Alias.CURRENT.getLabel())
                .join(stagingTable.alias(Alias.STAGE.getLabel()), col(stagingFunctionalName)
                        .equalTo(col(currentFunctionalName)), "outer");

    }

    /***
     *
     * @param fullJoinDataset
     * @param functionalName
     * @return get lines that are bot in staging and current table
     */
    private Dataset<Row> getLinesInStagingAndCurrentTable(Dataset<Row> fullJoinDataset, String functionalName) {

        String stagingFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.STAGE.getLabel(), functionalName);
        String currentFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.CURRENT.getLabel(), functionalName);

        return fullJoinDataset
                .filter(col(stagingFunctionalName).isNotNull()
                        .and(col(currentFunctionalName).isNotNull()));
    }

    /***
     *
     * @param fullJoinDataset
     * @param functionalName
     * @return get lines that are in staging table and not in current table.
     */
    private Dataset<Row> getNewLines(Dataset<Row> fullJoinDataset, String functionalName) {
        String currentFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.CURRENT.getLabel(), functionalName);

        return fullJoinDataset
                .filter(col(currentFunctionalName).isNull())
                .select(dimensionColumnSelector.getNewLinesColumns(inventoryDate, Alias.STAGE.getLabel()));
    }

    /***
     *
     * @param fullJoinDataset
     * @param functionalName
     * @param allColumnsFromCurrentAlias
     * @param dimensionEncoder
     * @return get lines that are in current table and not in staging table.
     */
    private Dataset<T> getLinesNotInStagingTable(Dataset<Row> fullJoinDataset, String functionalName, Column[] allColumnsFromCurrentAlias, Encoder<T> dimensionEncoder) {
        String stagingFunctionalName = dimensionColumnSelector.getColumnNameWithAlias(Alias.STAGE.getLabel(), functionalName);

        Dataset<Row> notInStaging = fullJoinDataset
                .filter(col(stagingFunctionalName).isNull())
                .select(allColumnsFromCurrentAlias);

        return notInStaging.as(dimensionEncoder);
    }


    private Dataset<T> getActiveLines(Dataset<T> currentTable, String currentFlagName) {
        return currentTable
                .filter(col(currentFlagName).equalTo(lit("Y")));
    }

    private Dataset<T> getInactiveLines(Dataset<T> currentTable, String currentFlagName, Encoder<T> dimensionEncoder) {
        return currentTable
                .filter(col(currentFlagName).equalTo(lit("N")))
                .select(dimensionColumnSelector.getInactiveLinesColumns())
                .as(dimensionEncoder);
    }
}
