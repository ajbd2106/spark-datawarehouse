package com.starschema.fact;

import com.sink.ISink;
import com.source.IFactSourceFactory;
import com.source.ISource;
import com.starschema.Alias;
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

import static org.apache.spark.sql.functions.broadcast;

@Slf4j
public class FactProcessor<T extends IFact> extends AbstractFactProcessor<T> {

    public FactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink, FactColumnSelector factColumnSelector) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, false, factColumnSelector);
    }

    public FactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink, boolean isHistoricalLoad, FactColumnSelector factColumnSelector) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, isHistoricalLoad, factColumnSelector);
    }

    @Override
    protected Dataset<Row> getFinalColumns(Dataset<Row> joinedFactTable, List<IDimensionRole> dimensionsRoles, Integer inventoryDateTechId, String inventoryDateFieldName) {
        return joinedFactTable.select(factColumnSelector.getFinalColumns(inventoryDateTechId, inventoryDateFieldName, dimensionsRoles, Alias.CURRENT.getLabel()));
    }

    @Override
    protected Class<?> getLookupType(IDimensionRole dimensionRole) {
        return dimensionRole.getFactLookupClass(isHistoricalLoad);
    }

    @Override
    protected Dataset<?> getLookupTableContent(Class<?> lookupType, Encoder<?> encoder, ISource source) {

        //if it's an historical load, don't get data from lookup table. Instead get the real dimension, and apply a filter to get the lines at the specified inventory date
        //if it's a junk dimension, as there are no start and end date, don't enter this condition.
        if (isHistoricalLoad && Dimension.class.isAssignableFrom(lookupType)) {
            return broadcast(source.load(sparkSession, encoder)
                    .filter(factColumnSelector.inventoryDateBetweenStartAndEndDate(inventoryDate, lookupType))
                    .select(factColumnSelector.getLookupTableColumns(lookupType)));

        } else {
            return broadcast(source.load(sparkSession, encoder));
        }
    }
}
