package com.hadoopunit.test.cucumber;

import com.hadoopunit.test.bean.dimension.DimensionAllTypesFieldsBean;
import com.hadoopunit.test.bean.dimension.DimensionFieldLessBean;
import com.hadoopunit.test.bean.dimension.DimensionSCD1Bean;
import com.hadoopunit.test.bean.dimension.DimensionSCD2Bean;
import com.hadoopunit.test.comparator.ReflectComparator;
import com.hadoopunit.test.sink.ListSink;
import com.hadoopunit.test.sources.BeanSourceMock;
import com.hadoopunit.test.spark.TestSparkSession;
import com.hadoopunit.test.utils.CucumberUtils;
import com.starschema.ProcessorFactory;
import com.starschema.dimension.Dimension;
import com.starschema.dimension.DimensionBean;
import com.starschema.dimension.DimensionProcessor;
import com.starschema.lookup.AbstractLookup;
import com.starschema.lookup.PartyLookup;
import cucumber.api.DataTable;
import cucumber.api.java8.En;
import org.apache.commons.collections.ListUtils;
import org.junit.Assert;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

public class DimensionStepDefs implements En {

    BeanDimensionStepDefs beanDimensionStepDefs;

    public DimensionStepDefs() {

        Before(() -> {
            if (beanDimensionStepDefs != null) {
                beanDimensionStepDefs.clear();
            }
        });

        Given("^a bean with scd1 and scd2 fields$", () -> {
            beanDimensionStepDefs = new BeanDimensionStepDefs(DimensionBean.class, PartyLookup.class);
        });

        Given("^a bean with SCD1 only$", () -> {
            beanDimensionStepDefs = new BeanDimensionStepDefs(DimensionSCD1Bean.class, PartyLookup.class);
        });

        Given("^a bean with SCD2 only$", () -> {
            beanDimensionStepDefs = new BeanDimensionStepDefs(DimensionSCD2Bean.class, PartyLookup.class);
        });

        Given("^a bean with no SCD1 and SCD2 fields$", () -> {
            beanDimensionStepDefs = new BeanDimensionStepDefs(DimensionFieldLessBean.class, PartyLookup.class);
        });

        Given("^a bean with all type fields$", () -> {
            beanDimensionStepDefs = new BeanDimensionStepDefs(DimensionAllTypesFieldsBean.class, PartyLookup.class);
        });

        Given("^this current dimension data$", (DataTable currentDimensionData) -> {
            beanDimensionStepDefs.createDimensionBeanSourceMock(currentDimensionData);
        });

        Given("^staging data$", (DataTable stagingDimensionData) -> {
            beanDimensionStepDefs.createStagingBeanSourceMock(stagingDimensionData);
        });

        When("^i process dimension as of date \"([^\"]*)\"$", (String inventoryDate) -> {
            beanDimensionStepDefs.createDimensionProcessor(inventoryDate).process();
        });

        Then("^i should obtain these old updated dimension lines$", (DataTable expectedLines) -> {
            beanDimensionStepDefs.assertExpectedLinesContainedInDataset(expectedLines);
        });

        Then("^those new dimension lines$", (DataTable expectedLines) -> {
            beanDimensionStepDefs.assertExpectedLinesContainedInDatasetWithComparator(expectedLines);
        });

        Then("^dimension lines should have all distinct technical ids$", () -> {
            beanDimensionStepDefs.assertDimensionLinesHaveDistinctTechnicalIds();
        });

        Then("^dimension look up must contain$", (DataTable expectedLines) -> {
            beanDimensionStepDefs.assertExpectedLinesEqualsDataset(expectedLines);
        });

        Then("^no lines should be added to dimension$", () -> {
            beanDimensionStepDefs.assertNoLinesToChange();
        });
    }


    private class BeanDimensionStepDefs<T extends Dimension, U extends AbstractLookup> {

        private final DateTimeFormatter localDateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        private final Class<T> targetClass;
        private final Class<U> targetLookupClass;

        private BeanSourceMock<T> currentDimensionMockSource;
        private BeanSourceMock<T> stagingDimensionMockSource;

        private ListSink dimensionSink = new ListSink();
        private ListSink dimensionLookupSink = new ListSink();

        public BeanDimensionStepDefs(Class<T> targetClass, Class<U> targetLookupClass) {
            this.targetClass = targetClass;
            this.targetLookupClass = targetLookupClass;
        }

        public void clear() {
            dimensionSink.clear();
            dimensionLookupSink.clear();
        }


        public void createDimensionBeanSourceMock(DataTable currentDimensionData) {
            currentDimensionMockSource = new BeanSourceMock(targetClass, CucumberUtils.convertDataTableToObject(currentDimensionData, targetClass));
        }

        public void createStagingBeanSourceMock(DataTable currentDimensionData) {
            stagingDimensionMockSource = new BeanSourceMock(targetClass, CucumberUtils.convertDataTableToObject(currentDimensionData, targetClass));
        }

        public DimensionProcessor<T> createDimensionProcessor(String inventoryDate) {
            return ProcessorFactory.createDimensionProcessor(TestSparkSession.getInstance().getSparkSession(),
                    targetClass,
                    currentDimensionMockSource,
                    stagingDimensionMockSource,
                    dimensionSink,
                    dimensionLookupSink,
                    LocalDate.parse(inventoryDate, localDateFormatter)
            );
        }

        public void assertExpectedLinesContainedInDataset(DataTable expectedLines) {
            List<T> expectedLinesList = CucumberUtils.convertDataTableToObject(expectedLines, targetClass);
            List<T> currentLinesList = CucumberUtils.convertRowDatasetToObject(expectedLines, dimensionSink.getInsertResult(), targetClass);
            List<T> retainedList = ListUtils.retainAll(currentLinesList, expectedLinesList);
            Assert.assertTrue("expected lines does not match current lines", retainedList.size() == expectedLinesList.size());
        }

        public void assertExpectedLinesContainedInDatasetWithComparator(DataTable expectedLines) {
            List<T> expectedLinesList = CucumberUtils.convertDataTableToObject(expectedLines, targetClass);
            List<T> currentLinesList = CucumberUtils.convertRowDatasetToObject(null, dimensionSink.getInsertResult(), targetClass);
            ReflectComparator<T> reflectComparator = new ReflectComparator(CucumberUtils.getDatatableHeader(expectedLines).values(), targetClass);
            List<T> retainedList = reflectComparator.retainAll(currentLinesList, expectedLinesList);
            Assert.assertTrue("expected lines does not match current lines", retainedList.size() == expectedLinesList.size());
        }

        public void assertExpectedLinesEqualsDataset(DataTable expectedLines) {
            CucumberUtils.compareDataTableAndRow(expectedLines,
                    dimensionLookupSink.getInsertResult(),
                    targetLookupClass);
        }

        public void assertDimensionLinesHaveDistinctTechnicalIds() {
            List<T> currentLinesList = CucumberUtils.convertRowDatasetToObject(null, dimensionSink.getInsertResult(), targetClass);
            List<Long> ids = currentLinesList.stream().map(dimensionBean -> dimensionBean.getId()).distinct().collect(Collectors.toList());
            Assert.assertTrue("current lines don't have distinct ids", currentLinesList.size() == ids.size());
        }

        public void assertNoLinesToChange() {
            Assert.assertTrue("no lines should be inserted in lookup dimension", dimensionLookupSink.getInsertResult().size() == 0);
            Assert.assertTrue("no lines should be inserted in dimension", dimensionSink.getInsertResult().size() == 0);
        }
    }


}
