package com.starschema.fact;

import com.sink.ISink;
import com.source.IFactSourceFactory;
import com.source.ISource;
import com.starschema.columnSelector.DimensionColumnSelector;
import com.starschema.columnSelector.FactColumnSelector;
import com.starschema.dimension.Dimension;
import com.starschema.dimension.role.IDimensionRole;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.util.List;

@Slf4j
public class FlattenedFactProcessor<T extends IFact> extends AbstractFactProcessor<T> {

    public FlattenedFactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, false);
    }

    public FlattenedFactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink, boolean isHistoricalLoad) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, isHistoricalLoad);
    }

    @Override
    protected Dataset<Row> getFinalColumns(Dataset<Row> joinedFactTable, List<IDimensionRole> dimensionsRoles, Integer inventoryDateTechId, String inventoryDateFieldName) {
        return joinedFactTable.select(FactColumnSelector.getFlattenedDimensionColumns(factClass, dimensionsRoles, inventoryDateTechId, inventoryDateFieldName));
    }

    @Override
    protected Class<?> getLookupType(IDimensionRole dimensionRole) {
        return dimensionRole.getMasterTableClass();
    }

    @Override
    protected Dataset<?> getLookupTableContent(Class<?> lookupType, Encoder<?> encoder, ISource source) {

        //TODO: we should execute a 'cluster by' technical id on table content, to avoid unnecessary shuffles (especially, if there are multiple roles). Not necessary on hive is table is already bucketed
        //TODO: Do a pre-filter on this data on all the functional ids contained in fact table, it could make the join faster (depending on how many lines are filtered).

        //Command to cluster is : df.repartition(col(TECHNICAL_ID)).sortWithinPartitions()

        //if lookup type is not assignable from dimension, we cannot filter by start date and end date, so we took the table without any filter
        if (!Dimension.class.isAssignableFrom(lookupType)) {
            return source.load(sparkSession, encoder);
        }

        return source.load(sparkSession, encoder)
                .filter(DimensionColumnSelector.inventoryDateBetweenStartAndEndDate(inventoryDate, (Class<? extends Dimension>) lookupType));
    }
}
