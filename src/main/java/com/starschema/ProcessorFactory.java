package com.starschema;

import com.sink.ISink;
import com.sink.SinkFactory;
import com.sink.SinkType;
import com.source.*;
import com.starschema.annotations.general.Table;
import com.starschema.dimension.Dimension;
import com.starschema.dimension.DimensionProcessor;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.dimension.junk.JunkDimensionProcessor;
import com.starschema.fact.FactProcessor;
import com.starschema.fact.FlattenedFactProcessor;
import com.starschema.fact.IFact;
import com.starschema.lookup.AbstractLookup;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;

public class ProcessorFactory {

    public static <T extends Dimension> DimensionProcessor<T> createDimensionProcessor(SparkSession sparkSession, Class<T> dimensionClass, ISource masterSource, ISource stagingSource, ISink dimensionSink, ISink lookupSink, LocalDate inventoryDate) {
        return new DimensionProcessor(sparkSession, dimensionClass, dimensionSink, lookupSink, masterSource, stagingSource, inventoryDate);
    }

    public static <T extends IJunkDimension> JunkDimensionProcessor<T> createJunkDimensionProcessor(SparkSession sparkSession, Class<T> dimensionClass, LocalDate inventoryDate, ISource dimensionSource, ISource dimensionStagingSource, ISink dimensionSink, ISink lookupSink) {
        Class<? extends AbstractLookup> lookupClass = dimensionClass.getAnnotation(Table.class).lookupType();
        return new JunkDimensionProcessor(sparkSession, dimensionClass, lookupClass, inventoryDate, dimensionSource, dimensionStagingSource, dimensionSink, lookupSink);
    }

    public static <T extends IFact> FactProcessor<T> createFactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, ISource<T> factSource, IFactSourceFactory factSourceFactory, ISink factSink) {
        return new FactProcessor(sparkSession, factClass, inventoryDate, factSourceFactory, factSource, factSink, false);
    }

    public static <T extends IFact> FlattenedFactProcessor<T> createFlattenedFactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, ISource<T> factSource,  IFactSourceFactory factSourceFactory, ISink factSink) {
        return new FlattenedFactProcessor(sparkSession, factClass, inventoryDate, factSourceFactory, factSource, factSink);
    }
}
