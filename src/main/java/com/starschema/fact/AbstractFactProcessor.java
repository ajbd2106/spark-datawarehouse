package com.starschema.fact;

import com.sink.ISink;
import com.source.IFactSourceFactory;
import com.source.ISource;
import com.source.SourceFactory;
import com.starschema.Processor;
import com.starschema.ProcessorFactory;
import com.starschema.annotations.dimensions.*;
import com.starschema.annotations.facts.InventoryDate;
import com.starschema.columnSelector.CommonColumnSelector;
import com.starschema.columnSelector.FactColumnSelector;
import com.starschema.columnSelector.JunkDimensionColumnSelector;
import com.starschema.dimension.junk.IJunkDimension;
import com.starschema.dimension.junk.JunkDimensionProcessor;
import com.starschema.dimension.role.IDimensionRole;
import com.utils.ReflectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public abstract class AbstractFactProcessor<T extends IFact> implements Processor {

    protected final DateTimeFormatter inventoryDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    protected final transient SparkSession sparkSession;
    protected final Class<T> factClass;
    protected final ISource factStagingSource;
    protected final IFactSourceFactory factSourceFactory;
    protected final ISink factSink;
    protected final LocalDate inventoryDate;
    protected final boolean isHistoricalLoad;

    public AbstractFactProcessor(SparkSession sparkSession, Class<T> factClass, LocalDate inventoryDate, IFactSourceFactory factSourceFactory, ISource<T> factStagingSource, ISink factSink, boolean isHistoricalLoad) {
        this.sparkSession = sparkSession;
        this.factClass = factClass;
        this.factStagingSource = factStagingSource;
        this.factSourceFactory = factSourceFactory;
        this.factSink = factSink;
        this.inventoryDate = inventoryDate;
        this.isHistoricalLoad = isHistoricalLoad;
    }

    @Override
    public void process() {

        Encoder<T> factEncoder = Encoders.bean(factClass);
        Dataset<T> factStagingData = factStagingSource.load(sparkSession, factEncoder);

        //process junk dimensions, if there are any declared in fact table (with annotation @FactJunkDimension)
        Map<Class, List<Field>> junkDimensions = FactColumnSelector.getJunkDimensions(factClass);
        for (Map.Entry<Class, List<Field>> entry : junkDimensions.entrySet()) {
            processJunkDimension(entry, factStagingData);
        }

        //get all possible lookups to look at. It may be a lookup dimension table or a junk dimension table
        List<IDimensionRole> dimensionsRoles = FactColumnSelector.getLookupDimension(factClass);

        //here we join the fact table with all the dimensions
        Dataset<Row> joinedFactTable = joinFactTableWithDimension(factStagingData, dimensionsRoles);

        Integer inventoryDateTechId = Integer.valueOf(inventoryDateFormatter.format(inventoryDate));
        String inventoryDateFieldName = FactColumnSelector.getInventoryDateName(factClass);

        //finally collect the result. if technical id has not been found, put one by default.
        //if technical id has been found, copy it in the fact table appropriate field
        Dataset<Row> finalFactTable = getFinalColumns(joinedFactTable, dimensionsRoles, inventoryDateTechId, inventoryDateFieldName);

        factSink.delete(inventoryDateTechId, inventoryDateFieldName);
        //TODO: to be tuned
        finalFactTable.coalesce(2);

        //Save data in fact table
        factSink.insert(finalFactTable);
    }

    protected abstract Dataset<Row> getFinalColumns(Dataset<Row> joinedFactTable, List<IDimensionRole> dimensionsRoles, Integer inventoryDateTechId, String inventoryDateFieldName);

    private Dataset<Row> joinFactTableWithDimension(Dataset<T> factTable, List<IDimensionRole> allDimensionsRoles) {
        Dataset<Row> joinedFactTable = factTable.select(factTable.alias(CommonColumnSelector.ALIAS_CURRENT).col("*"));

        Map<Class<?>, List<IDimensionRole>> dimensionGroupedByRole = allDimensionsRoles
                .stream()
                .collect(Collectors.groupingBy(role -> getLookupType(role)));

        //for each role type, get the lookup table, and for each role, make a join between fact table and lookup table
        for (Map.Entry<Class<?>, List<IDimensionRole>> dimensionRoleEntry : dimensionGroupedByRole.entrySet()) {

            Class<?> lookupType = dimensionRoleEntry.getKey();
            Dataset<?> lookupTable = getLookupTable(lookupType);

            for (IDimensionRole dimensionRole : dimensionRoleEntry.getValue()) {
                Column factFunctionalId = dimensionRole.getFunctionalId(CommonColumnSelector.ALIAS_CURRENT);
                String lookupTableAlias = dimensionRole.getAlias();
                String lookUpFunctionalIdField = FactColumnSelector.getLookupFunctionalId(lookupType, lookupTableAlias);

                //joining with lookup functional id and field name in fact table
                joinedFactTable = joinedFactTable.join(lookupTable.alias(lookupTableAlias),
                        col(lookUpFunctionalIdField).equalTo(factFunctionalId), "left_outer");
            }
        }

        return joinedFactTable;
    }

    protected abstract Class<?> getLookupType(IDimensionRole dimensionRole);

    private Dataset<?> getLookupTable(Class<?> lookupType) {
        ISource source = factSourceFactory.getLookupSource(lookupType);
        Encoder<?> encoder = Encoders.bean(lookupType);
        Dataset<?> sourceData = getLookupTableContent(lookupType, encoder, source);
        return sourceData;
    }

    protected abstract Dataset<?> getLookupTableContent(Class<?> lookupType, Encoder<?> encoder, ISource source);

    private void processJunkDimension(Map.Entry<Class, List<Field>> currentDimensionEntry, Dataset<T> factTable) {

        Class<? extends IJunkDimension> junkDimensionClass = currentDimensionEntry.getKey();

        if (!IJunkDimension.class.isAssignableFrom(junkDimensionClass)) {
            List<String> fieldList = currentDimensionEntry.getValue()
                    .stream()
                    .map(field -> field.getName())
                    .collect(Collectors.toList());

            throw new RuntimeException(String.format("Fields %s are of Type %s . This Type must implement an IJunkDimension",
                    StringUtils.join(fieldList, ";"),
                    currentDimensionEntry.getKey().getCanonicalName()));
        }

        Dataset<Row> junkDimensionDataSet = getJunkDimensionDataset(currentDimensionEntry, factTable);

        processJunkDimension(junkDimensionClass, junkDimensionDataSet);
    }

    /***
     *
     * @param junkDimensionClass
     * @param junkDimensionDataSet
     *
     * here we just instantiate the junk dimension processor, and run it. It should create new lines if there are some.
     */
    private void processJunkDimension(Class<? extends IJunkDimension> junkDimensionClass, Dataset<Row> junkDimensionDataSet) {
        JunkDimensionProcessor dimensionProcessor = ProcessorFactory.createJunkDimensionProcessor(sparkSession, junkDimensionClass, inventoryDate,
                factSourceFactory.getJunkDimensionSource(junkDimensionClass),
                SourceFactory.createRowDataSetSource(junkDimensionDataSet),
                factSourceFactory.getJunkDimensionSink(junkDimensionClass),
                factSourceFactory.getJunkDimensionLookupSink(junkDimensionClass)
        );
        dimensionProcessor.process();
    }

    /***
     *
     * @param currentDimensionEntry
     * @param factTable
     * @return in this function, we get all the distinct values of the junk dimension.
     * As several roles can be linked to the same dimension, we want to get all the possible values among all the roles,
     * and get only distinct values. The purpose is here to insert lines that does not exist yet in the junk dimension
     */
    private Dataset<Row> getJunkDimensionDataset(Map.Entry<Class, List<Field>> currentDimensionEntry, Dataset<T> factTable) {
        //here we get only the different role fields that share the same junk dimension,
        Column[] columns = currentDimensionEntry.getValue().stream()
                .map(field -> col(field.getName()))
                .toArray(size -> new Column[size]);

        //here we put all the lines in a single dataset, (with explode keyword), and then we select distinct values only.
        //spark return them with a col alias, so we have to remove it before going further.
        return factTable
                .select(explode(array(columns)))
                .distinct()
                .select(JunkDimensionColumnSelector.getAllJunkDimensionColumnsAsOriginal("col", currentDimensionEntry.getKey()));
    }

}
