package com.starschema.fact;

import com.sink.ISink;
import com.source.IFactSourceFactory;
import com.source.ISource;
import com.source.SourceFactory;
import com.starschema.ProcessorFactory;
import com.starschema.annotations.general.Table;
import com.starschema.columnSelector.DimensionColumnSelector;
import com.starschema.columnSelector.FactColumnSelector;
import com.starschema.columnSelector.JunkDimensionColumnSelector;
import com.starschema.dimension.Dimension;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.dimension.junk.JunkDimensionProcessor;
import com.starschema.dimension.role.IDimensionRole;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class FactProcessor<T extends IFact> extends AbstractFactProcessor<T> {

    public FactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, false);
    }

    public FactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink, boolean isHistoricalLoad) {
        super(sparkSession, factClass, inventoryDate, factSourceFactory, factStagingSource, factSink, isHistoricalLoad);
    }

    @Override
    protected Dataset<Row> getFinalColumns(Dataset<Row> joinedFactTable, List<IDimensionRole> dimensionsRoles, Integer inventoryDateTechId, String inventoryDateFieldName) {
        return joinedFactTable.select(FactColumnSelector.getFinalColumns(inventoryDateTechId, inventoryDateFieldName, dimensionsRoles, factClass));
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
                    .filter(DimensionColumnSelector.inventoryDateBetweenStartAndEndDate(inventoryDate, (Class<? extends Dimension>) lookupType))
                    .select(DimensionColumnSelector.getLookupTableColumns((Class<? extends Dimension>) lookupType)));

        } else {
            return broadcast(source.load(sparkSession, encoder));
        }
    }
}
