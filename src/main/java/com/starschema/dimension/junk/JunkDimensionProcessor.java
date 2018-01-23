package com.starschema.dimension.junk;

import com.sink.ISink;
import com.source.ISource;
import com.spark.SparkFunctionUtils;
import com.starschema.Processor;
import com.starschema.annotations.dimensions.CheckSum;
import com.starschema.annotations.dimensions.FunctionalId;
import com.starschema.annotations.dimensions.TechnicalId;
import com.starschema.annotations.dimensions.UpdatedDate;
import com.starschema.columnSelector.CommonColumnSelector;
import com.starschema.columnSelector.JunkDimensionColumnSelector;
import com.starschema.lookup.AbstractLookup;
import com.utils.ReflectUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;

import java.time.LocalDate;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;

@Slf4j
public class JunkDimensionProcessor<T extends IJunkDimension> implements Processor {

    private final transient SparkSession sparkSession;
    private final Class<T> dimensionClass;
    private final Class<? extends AbstractLookup> lookupClass;
    private final ISource dimensionSource;
    private final ISource dimensionStagingSource;
    private final ISink dimensionSink;
    private final ISink lookupSink;
    private final LocalDate inventoryDate;


    public JunkDimensionProcessor(SparkSession sparkSession, Class<T> dimensionClass, Class<? extends AbstractLookup> lookupClass, LocalDate inventoryDate, ISource dimensionSource, ISource dimensionStagingSource, ISink dimensionSink, ISink lookupSink) {
        validateProcessorClass(dimensionClass);
        this.dimensionClass = dimensionClass;
        this.lookupClass = lookupClass;
        this.dimensionSource = dimensionSource;
        this.dimensionStagingSource = dimensionStagingSource;
        this.dimensionSink = dimensionSink;
        this.lookupSink = lookupSink;
        this.sparkSession = sparkSession;
        this.inventoryDate = inventoryDate;
    }

    private void validateProcessorClass(Class<T> dimensionClass) {
        ReflectUtils.checkExistsAndUnique(TechnicalId.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(FunctionalId.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(CheckSum.class, dimensionClass);
        ReflectUtils.checkExistsAndUnique(UpdatedDate.class, dimensionClass);
    }

    @Override
    public void process() {
        //get current Junk Dimension Data
        Dataset<Row> currentJunkDimension = dimensionSource.load(sparkSession);
        Dataset<Row> stagingJunkDimension = dimensionStagingSource.load(sparkSession);

        String technicalIdName = JunkDimensionColumnSelector.getTechnicalIdName(dimensionClass);

        //as the source table is a lookup the checksum is the functional id
        String currentCheckSumColumn = JunkDimensionColumnSelector.getFunctionalName(CommonColumnSelector.ALIAS_CURRENT, lookupClass);
        String stagingCheckSumColumn = JunkDimensionColumnSelector.getCheckSumColumn(CommonColumnSelector.ALIAS_STAGE, dimensionClass);

        Long maxId = SparkFunctionUtils.getMaxRowId(currentJunkDimension, technicalIdName);

        Encoder<T> dimensionEncoder = Encoders.bean(dimensionClass);

        Dataset<T> stagingData = stagingJunkDimension.select(JunkDimensionColumnSelector.getStagingColumns(dimensionClass, inventoryDate)).as(dimensionEncoder);

        //as the source table is a lookup the checksum is the functional id
        Dataset<Row> currentCheckSum = currentJunkDimension.select(JunkDimensionColumnSelector.getFunctionalName(lookupClass));

        Dataset<Row> newLines = stagingData.alias(CommonColumnSelector.ALIAS_STAGE)
                .join(broadcast(currentCheckSum).alias(CommonColumnSelector.ALIAS_CURRENT), col(stagingCheckSumColumn).equalTo(col(currentCheckSumColumn)), "left_outer")
                .filter(col(currentCheckSumColumn).isNull())
                .select(JunkDimensionColumnSelector.getNewLinesColumns(dimensionClass));

        Dataset<Row> finalLines = SparkFunctionUtils.generateTechnicalId(sparkSession, maxId, newLines);

        //if we don't cache, the save action in lookup sink does not work
        finalLines = finalLines.coalesce(2).cache();

        Dataset<Row> lookupData = getLookUpData(finalLines, lookupClass);

        dimensionSink.insert(finalLines);
        lookupSink.insert(lookupData);
    }

    private Dataset<Row> getLookUpData(Dataset<Row> allRows, Class<? extends AbstractLookup> lookupClass) {
        return allRows.select(JunkDimensionColumnSelector.getLookupTableColumns(lookupClass, dimensionClass));
    }


}
