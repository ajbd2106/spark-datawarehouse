package com.hadoopunit.test.cucumber;

import com.hadoopunit.test.bean.fact.FlattenedFactTable;
import com.hadoopunit.test.sink.ListSink;
import com.hadoopunit.test.sources.BeanSourceMock;
import com.hadoopunit.test.sources.MockFactSourceFactory;
import com.hadoopunit.test.spark.TestSparkSession;
import com.hadoopunit.test.utils.CucumberUtils;
import com.starschema.ProcessorFactory;
import com.starschema.columnSelector.AnnotatedFactColumnSelector;
import com.starschema.dimension.DimensionBean;
import com.starschema.dimension.junk.JunkDimension;
import com.starschema.fact.AbstractFactProcessor;
import com.starschema.fact.FactTable;
import com.starschema.lookup.JunkDimensionLookup;
import com.starschema.lookup.PartyLookup;
import cucumber.api.DataTable;
import cucumber.api.java8.En;
import org.junit.Assert;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FactStepDefs implements En {

    private static final DateTimeFormatter localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    MockFactSourceFactory mockFactSourceFactory = new MockFactSourceFactory();
    BeanSourceMock<FactTable> factStagingData;

    ListSink factSink = new ListSink();

    AbstractFactProcessor<FactTable> factProcessor;

    public FactStepDefs() {

        Before(() -> {
            factSink.clear();
            mockFactSourceFactory.clear();
            if (factStagingData != null) {
                factStagingData.clear();
            }

        });

        Given("^current dimension data$", (DataTable currentDimensionData) -> {
            mockFactSourceFactory.getSourceMap().put(DimensionBean.class.getSimpleName(),
                    new BeanSourceMock<>(DimensionBean.class, CucumberUtils.convertDataTableToObject(currentDimensionData, DimensionBean.class))
            );
        });


        Given("^this current lookup dimension data$", (DataTable dimensionLookupDataTable) -> {
            mockFactSourceFactory.getSourceMap().put(PartyLookup.class.getSimpleName(),
                    new BeanSourceMock<>(PartyLookup.class, dimensionLookupDataTable.asList(PartyLookup.class))
            );
        });

        Given("^this current junk dimension data$", (DataTable junkDimensionDataTable) -> {
            mockFactSourceFactory.getSourceMap().put(JunkDimension.class.getSimpleName(),
                    new BeanSourceMock<>(JunkDimension.class, CucumberUtils.convertDataTableToObject(junkDimensionDataTable, JunkDimension.class))
            );
        });

        Given("^this current junk lookup dimension data$", (DataTable junkDimensionLookupDataTable) -> {
            mockFactSourceFactory.getSourceMap().put(JunkDimensionLookup.class.getSimpleName(),
                    new BeanSourceMock<>(JunkDimensionLookup.class, junkDimensionLookupDataTable.asList(JunkDimensionLookup.class)));
        });

        Given("^this current fact data$", (DataTable factTableData) -> {
            factStagingData = new BeanSourceMock<>(FactTable.class, CucumberUtils.convertDataTableToObject(factTableData, FactTable.class));
        });

        When("^i process fact with date as of \"([^\"]*)\"$", (String inventoryDate) -> {
            factProcessor = ProcessorFactory.createFactProcessor(TestSparkSession.getInstance().getSparkSession(), FactTable.class, LocalDate.parse(inventoryDate, localDateFormatter), factStagingData, mockFactSourceFactory, factSink,
                    new AnnotatedFactColumnSelector(FactTable.class)
            );
            factProcessor.process();
        });

        When("^i process fact with date as of \"([^\"]*)\" and flattenize option$", (String inventoryDate) -> {
            factProcessor = ProcessorFactory.createFlattenedFactProcessor(TestSparkSession.getInstance().getSparkSession(), FactTable.class, LocalDate.parse(inventoryDate, localDateFormatter), factStagingData, mockFactSourceFactory, factSink,
                    new AnnotatedFactColumnSelector(FactTable.class));
            factProcessor.process();
        });

        Then("^i should add following junk dimension data$", (DataTable expectedLines) -> {
            CucumberUtils.compareDataTableAndRow(expectedLines,
                    mockFactSourceFactory.getSinkMap().get(JunkDimension.class.getSimpleName()).getInsertResult(),
                    JunkDimension.class);
        });

        Then("^i should add following junk dimension lookup data$", (DataTable expectedLines) -> {
            CucumberUtils.compareDataTableAndRow(expectedLines,
                    mockFactSourceFactory.getSinkMap().get(JunkDimensionLookup.class.getSimpleName()).getInsertResult(),
                    JunkDimensionLookup.class);
        });

        Then("^no lines should be added to junk dimension data$", () -> {
            int sinkSize = mockFactSourceFactory.getSinkMap().get(JunkDimension.class.getSimpleName()).getInsertResult().size();
            Assert.assertTrue(String.format("%d lines will be added to junk dimension data. Expected 0", sinkSize), sinkSize == 0);
        });

        Then("^no lines should be added to junk lookup dimension data$", () -> {
            int sinkSize = mockFactSourceFactory.getSinkMap().get(JunkDimensionLookup.class.getSimpleName()).getInsertResult().size();
            Assert.assertTrue(String.format("%d lines will be added to junk dimension data. Expected 0", sinkSize), sinkSize == 0);
        });

        Then("^i should get this fact table data$", (DataTable expectedLines) -> {
            CucumberUtils.compareDataTableAndRow(expectedLines, factSink.getInsertResult(), FactTable.class);
        });

        Then("^i should get this flattened fact table data$", (DataTable expectedLines) -> {
            CucumberUtils.compareDataTableAndRow(expectedLines, factSink.getInsertResult(), FlattenedFactTable.class);
        });

    }


}
